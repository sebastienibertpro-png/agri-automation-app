"""
ephy_fetcher.py
===============
Module de récupération et parsing des données E-Phy (ANSES)
Source : data.gouv.fr - Données ouvertes catalogue E-Phy
URL ZIP (UTF-8) : https://www.data.gouv.fr/api/1/datasets/r/cb51408e-2b97-43a4-94e2-c0de5c3bf5b2

Fournit :
- EphyFetcher.refresh()              → télécharge le ZIP si absent ou > 7 jours
- EphyFetcher.search(nom_commercial) → retourne dict pour REF_INTRANTS + liste usages pour REF_USAGES_PHYTO
"""

import os
import re
import zipfile
import io
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from rapidfuzz import process, fuzz

logger = logging.getLogger(__name__)

# --- CONSTANTES ---
EPHY_ZIP_URL = "https://www.data.gouv.fr/api/1/datasets/r/cb51408e-2b97-43a4-94e2-c0de5c3bf5b2"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "_ephy_cache")
CACHE_PRODUITS = os.path.join(CACHE_DIR, "produits.parquet")
CACHE_USAGES   = os.path.join(CACHE_DIR, "usages.parquet")
CACHE_DATE_FILE = os.path.join(CACHE_DIR, "last_update.txt")
REFRESH_DAYS = 60  # Rafraîchissement tous les 2 mois


# ---------------------------------------------------------------------------
# Mapping colonnes CSV E-Phy → colonnes REF_INTRANTS / REF_USAGES_PHYTO
# Les noms de colonnes réels dans les CSV E-Phy peuvent varier légèrement.
# On utilise une détection souple par mots-clés.
# ---------------------------------------------------------------------------

class EphyFetcher:
    """
    Gère le téléchargement, le parsing et l'indexation du référentiel E-Phy.
    Conçu pour être instancié une seule fois (ex: dans app.py ou session Streamlit).
    """

    def __init__(self, auto_refresh: bool = True):
        os.makedirs(CACHE_DIR, exist_ok=True)
        self._df_produits: pd.DataFrame = pd.DataFrame()
        self._df_usages: pd.DataFrame = pd.DataFrame()
        if auto_refresh:
            self.refresh()

    # ------------------------------------------------------------------
    # 1. TÉLÉCHARGEMENT & PARSING
    # ------------------------------------------------------------------

    def refresh(self, force: bool = False) -> bool:
        """
        Vérifie si le cache est à jour (< 7 jours).
        Si non (ou force=True), télécharge le ZIP E-Phy et reparse les CSV.
        Retourne True si succès, False sinon.
        """
        if not force and self._is_cache_fresh():
            self._load_cache()
            logger.info("Cache E-Phy frais, chargement depuis le disque.")
            return True

        logger.info("Téléchargement du référentiel E-Phy depuis data.gouv.fr...")
        try:
            resp = requests.get(EPHY_ZIP_URL, timeout=60, stream=True)
            resp.raise_for_status()
            zip_bytes = io.BytesIO(resp.content)
            self._parse_zip(zip_bytes)
            self._save_cache()
            self._write_date_file()
            logger.info("Référentiel E-Phy mis à jour avec succès.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur téléchargement E-Phy: {e}")
            # Fallback : charger le cache même périmé
            if os.path.exists(CACHE_PRODUITS):
                logger.warning("Utilisation du cache E-Phy périmé en fallback.")
                self._load_cache()
                return True
            return False

    def _is_cache_fresh(self) -> bool:
        if not os.path.exists(CACHE_DATE_FILE):
            return False
        if not os.path.exists(CACHE_PRODUITS):
            return False
        try:
            with open(CACHE_DATE_FILE, "r") as f:
                date_str = f.read().strip()
            last_update = datetime.strptime(date_str, "%Y-%m-%d")
            return (datetime.now() - last_update) < timedelta(days=REFRESH_DAYS)
        except Exception:
            return False

    def _write_date_file(self):
        with open(CACHE_DATE_FILE, "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))

    def _parse_zip(self, zip_bytes: io.BytesIO):
        """
        Extrait du ZIP les fichiers CSV E-Phy et les parse.
        Structure réelle du ZIP r2 (decisionamm-intrant-format-csv-UTF-8.zip):
          - produits.CSV                  -> infos produit (1 ligne/produit)
          - produits_usages.CSV           -> usages avec doses/cibles (1 ligne/usage)
          - usages_des_produits_autorises -> usages uniquement autorisés
          - produits_condition_emploi.CSV -> conditions d'emploi
          - classe_et_mention_danger.CSV  -> H-phrases et classements
          - produits_phrases_de_risque.CSV
          - substance_active.CSV
          - mfsc_et_mixte_*.CSV           -> engrais/mélanges (HORS SCOPE)
        """
        with zipfile.ZipFile(zip_bytes, "r") as zf:
            names = zf.namelist()
            logger.info(f"Fichiers dans le ZIP E-Phy: {names}")

            # Noms exacts (insensible à la casse) - les fichiers ont le suffixe _utf8.csv
            produits_file   = self._find_file_exact(names, ["produits_utf8.csv", "produits.csv"])
            # produits_usages inclut usages autorisés ET retirés - plus complet
            conditions_file = self._find_file_exact(names, [
                "produits_usages_utf8.csv",
                "usages_des_produits_autorises_utf8.csv",
                "produits_usages.csv",
                "usages_des_produits_autorises.csv",
            ])
            danger_file = self._find_file_exact(names, [
                "produits_classe_et_mention_danger_utf8.csv",
                "classe_et_mention_danger_utf8.csv",
                "classe_et_mention_danger.csv",
            ])

            logger.info(f"CSV produits: {produits_file}")
            logger.info(f"CSV usages: {conditions_file}")
            logger.info(f"CSV danger: {danger_file}")

            df_prod  = self._read_csv_from_zip(zf, produits_file)   if produits_file   else pd.DataFrame()
            df_cond  = self._read_csv_from_zip(zf, conditions_file) if conditions_file else pd.DataFrame()
            df_dang  = self._read_csv_from_zip(zf, danger_file)     if danger_file     else pd.DataFrame()

        self._df_produits, self._df_usages = self._build_tables(df_prod, df_cond, df_dang)

    def _find_file(self, names: list, keywords: list, exclude: list = None) -> str | None:
        for name in names:
            lower = name.lower()
            if not name.lower().endswith(".csv"):
                continue
            if all(kw in lower for kw in keywords):
                if exclude and any(ex in lower for ex in exclude):
                    continue
                return name
        # Fallback moins strict
        for name in names:
            lower = name.lower()
            if not name.lower().endswith(".csv"):
                continue
            if any(kw in lower for kw in keywords):
                return name
        return None

    def _find_file_exact(self, names: list, candidates: list) -> str | None:
        """Cherche un fichier ZIP parmi des noms exacts (insensible à la casse, suffix match)."""
        for cand in candidates:
            for name in names:
                if name.lower().endswith(cand.lower()):
                    return name
        return None

    def _read_csv_from_zip(self, zf: zipfile.ZipFile, filename: str) -> pd.DataFrame:
        """Lit un CSV depuis un ZipFile avec gestion d'encodage."""
        try:
            for enc in ["utf-8", "latin-1", "cp1252"]:
                try:
                    with zf.open(filename) as f:
                        df = pd.read_csv(f, sep=";", encoding=enc, dtype=str, low_memory=False)
                    logger.debug(f"  {filename} lu ({enc}): {len(df)} lignes, colonnes: {list(df.columns)}")
                    return df
                except UnicodeDecodeError:
                    continue
        except Exception as e:
            logger.error(f"Erreur lecture {filename}: {e}")
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # 2. CONSTRUCTION DES TABLES INDEXÉES
    # ------------------------------------------------------------------

    @staticmethod
    def _get_col(df: pd.DataFrame, *candidates: str) -> str | None:
        """Trouve la colonne dont le nom contient l'un des candidats (insensible à la casse)."""
        col_map = {str(c).lower().strip(): c for c in df.columns}
        for cand in candidates:
            for lc, orig in col_map.items():
                if cand.lower() in lc:
                    return orig
        return None

    def _build_tables(self, df_prod, df_cond, df_dang):
        """
        À partir des CSV bruts E-Phy, construit deux DataFrames normalisés :
        - df_intrants  : une ligne par produit (→ REF_INTRANTS)
        - df_usages    : une ligne par usage culture×cible (→ REF_USAGES_PHYTO)
        """
        if df_prod.empty:
            logger.warning("CSV produits E-Phy vide ou introuvable.")
            return pd.DataFrame(), pd.DataFrame()

        # Log des vraies colonnes pour debug
        logger.info(f"Colonnes CSV produits ({len(df_prod)}L): {list(df_prod.columns)[:15]}")
        logger.info(f"Colonnes CSV usages ({len(df_cond)}L): {list(df_cond.columns)[:10] if not df_cond.empty else 'vide'}")

        # --- Colonnes du CSV produits E-Phy (nouveau format 2026) ---
        c_nom   = self._get_col(df_prod, "nom produit", "nom commercial", "libelle", "denomination")
        c_sec   = self._get_col(df_prod, "seconds noms commerciaux", "second nom", "noms secondaires")
        c_amm   = self._get_col(df_prod, "numero amm", "numéro amm", "numero_amm", "amm", "num amm")
        c_type  = self._get_col(df_prod, "type produit", "type de produit", "categorie", "type")
        c_form  = self._get_col(df_prod, "formulation", "forme")
        c_titu  = self._get_col(df_prod, "titulaire", "firme", "societe", "company", "detenteur")
        c_sa    = self._get_col(df_prod, "substances actives", "substance active", "matiere active")
        c_etat  = self._get_col(df_prod, "etat d’autorisation", "etat d'autorisation", "etat", "statut", "situation")
        c_dfin  = self._get_col(df_prod, "date de retrait", "fin de validite", "date fin", "date_fin", "echeance", "retrait")

        # --- Table produits (REF_INTRANTS) ---
        records_prod = []
        for _, row in df_prod.iterrows():
            nom    = self._val(row, c_nom)
            amm    = self._val(row, c_amm)
            if not nom and not amm:
                continue

            # La SA concentre tout : "diméthoate (Dimethoate) 400.0 g/L | ..."
            sa     = self._val(row, c_sa)

            records_prod.append({
                "Nom_Produit":     nom,
                "Noms_Secondaires": self._val(row, c_sec),
                "N_AMM":           amm,
                "Type":            self._val(row, c_type),
                "Formulation":     self._val(row, c_form),
                "Titulaire_AMM":   self._val(row, c_titu),
                "Matieres_Actives": sa,
                "Concentration":   None,  # Intégré dans Matieres_Actives
                "Etat_AMM":        self._val(row, c_etat),
                "Date_Fin_AMM":    self._parse_date(self._val(row, c_dfin)),
                "Classement_CMR":  None,  # Rempli ensuite
                "Lien_Ephy":       self._build_ephy_link(nom),
                "Date_MAJ_Ephy":   datetime.now().strftime("%d/%m/%Y"),
            })

        df_intrants = pd.DataFrame(records_prod).drop_duplicates(subset=["N_AMM"]).reset_index(drop=True)

        # --- Table usages (REF_USAGES_PHYTO) depuis df_cond ---
        df_usages = pd.DataFrame()
        if not df_cond.empty:
            logger.info(f"Colonnes CSV usages: {list(df_cond.columns)[:15]}")
            # Nouveau format 2026: 'identifiant usage' = 'Artichaut*Trt Part.Aer.*Pucerons'
            # 'dose retenue', 'dose retenue unite', 'delai avant recolte jour', 'nombre max d'application', 'ZNT aquatique (en m)'
            c_amm_c   = self._get_col(df_cond, "numero amm", "numéro amm", "num amm", "amm")
            c_nom_c   = self._get_col(df_cond, "nom produit", "nom commercial", "libelle")
            c_ident   = self._get_col(df_cond, "identifiant usage", "usage")
            c_dose    = self._get_col(df_cond, "dose retenue", "dose max", "dose")
            c_unit_d  = self._get_col(df_cond, "dose retenue unite", "unite dose", "unité")
            c_napp    = self._get_col(df_cond, "nombre max d'application", "nombre max applic", "nombre maxi", "nb appli", "maxi")
            c_dar_c   = self._get_col(df_cond, "delai avant recolte jour", "dar", "delai avant recolte")
            c_dvp_c   = self._get_col(df_cond, "dvp", "dispositif vegetal", "haie")
            c_znt_c   = self._get_col(df_cond, "znt aquatique", "znt eau", "znt", "zone non")
            c_etat_c  = self._get_col(df_cond, "etat usage", "etat")

            records_usages = []
            for _, row in df_cond.iterrows():
                amm_u = self._val(row, c_amm_c)
                if not amm_u:
                    continue
                
                usage_str = self._val(row, c_ident)
                culture, cible = None, None
                if usage_str:
                    parts = usage_str.split('*')
                    culture = parts[0].strip() if len(parts) > 0 else None
                    cible = parts[-1].strip() if len(parts) > 1 else None

                records_usages.append({
                    "N_AMM":               amm_u,
                    "Nom_Produit":         self._val(row, c_nom_c),
                    "Culture":             culture,
                    "Cible":               cible,
                    "Type_Cible":          usage_str,
                    "Dose_Max":            self._val(row, c_dose),
                    "Unite_Dose":          self._val(row, c_unit_d),
                    "Nb_Applications_Max": self._val(row, c_napp),
                    "DAR":                 self._val(row, c_dar_c),
                    "DVP":                 self._val(row, c_dvp_c),
                    "ZNT_Aqua":            self._val(row, c_znt_c),
                    "Etat_Usage":          self._val(row, c_etat_c),
                })

            df_usages = pd.DataFrame(records_usages).reset_index(drop=True)

            # Enrichir df_intrants avec ZNT/DAR/DVP agrégés depuis les usages
            if not df_usages.empty:
                df_intrants = self._enrich_intrants(df_intrants, df_usages)

        # Enrichir mentions de danger depuis df_dang
        if not df_dang.empty:
            df_intrants = self._enrich_danger(df_intrants, df_dang)

        return df_intrants, df_usages

    def _enrich_intrants(self, df_intrants: pd.DataFrame, df_usages: pd.DataFrame) -> pd.DataFrame:
        """
        Pour chaque produit dans df_intrants, calcule les valeurs agrégées
        (DAR max, ZNT max, DVP, dose max, nb applis max) depuis df_usages.
        """
        if df_usages.empty or df_intrants.empty:
            return df_intrants

        # Helper: valeur numérique max
        def col_max(serie):
            return pd.to_numeric(serie, errors="coerce").max()

        agg = {}
        for amm, grp in df_usages.groupby("N_AMM"):
            agg[str(amm)] = {
                "DAR":                 col_max(grp["DAR"]) if "DAR" in grp else None,
                "ZNT_Aqua":            col_max(grp["ZNT_Aqua"]) if "ZNT_Aqua" in grp else None,
                "DVP":                 grp["DVP"].dropna().mode()[0] if "DVP" in grp and not grp["DVP"].dropna().empty else None,
                "Dose_Max_Homologuee": col_max(grp["Dose_Max"]) if "Dose_Max" in grp else None,
                "Unité_utilisation":   grp["Unite_Dose"].mode()[0] if "Unite_Dose" in grp and not grp["Unite_Dose"].dropna().empty else None,
                "Nb_Applications_Max_An": col_max(grp["Nb_Applications_Max"]) if "Nb_Applications_Max" in grp else None,
                "Culture":             ", ".join(sorted(grp["Culture"].dropna().unique().tolist())[:5]) if "Culture" in grp else None,
            }

        def enrich_row(row):
            data = agg.get(str(row["N_AMM"]), {})
            for col, val in data.items():
                if col not in row.index or pd.isna(row.get(col, None)):
                    row[col] = val
            return row

        return df_intrants.apply(enrich_row, axis=1)

    def _enrich_danger(self, df_intrants: pd.DataFrame, df_dang: pd.DataFrame) -> pd.DataFrame:
        """Ajoute Mentions_Danger et CMR depuis le CSV de classement."""
        if df_dang.empty or df_intrants.empty:
            return df_intrants

        c_amm_d = self._get_col(df_dang, "numero amm", "amm")
        c_court = self._get_col(df_dang, "libellé court", "libelle court", "court", "phrase")
        c_zntriv = self._get_col(df_dang, "riverain", "rive")

        if not c_amm_d:
            return df_intrants

        dang_map = {}
        for amm, grp in df_dang.groupby(c_amm_d):
            entry = {}
            if c_court:
                mots = sorted(grp[c_court].dropna().unique().tolist())
                entry["Mentions_Danger"] = ", ".join(mots)
                # Détection CMR si présence de 'C', 'M', 'R' seuls (CMR 1, 2 etc) 
                # ou H350/H351 etc
                cmrs = [m for m in mots if m in ('C1A', 'C1B', 'C2', 'M1A', 'M1B', 'M2', 'R1A', 'R1B', 'R2')]
                if cmrs:
                    entry["Classement_CMR"] = ", ".join(cmrs)
            
            if c_zntriv:
                vals = pd.to_numeric(grp[c_zntriv], errors="coerce")
                entry["ZNT_Riverains"] = vals.max() if not vals.isna().all() else None
            
            dang_map[str(amm)] = entry

        def add_danger(row):
            data = dang_map.get(str(row["N_AMM"]), {})
            for col, val in data.items():
                if col not in row.index or pd.isna(row.get(col, None)):
                    row[col] = val
            return row

        return df_intrants.apply(add_danger, axis=1)

    # ------------------------------------------------------------------
    # 3. CACHE DISQUE
    # ------------------------------------------------------------------

    def _save_cache(self):
        try:
            if not self._df_produits.empty:
                self._df_produits.to_parquet(CACHE_PRODUITS, index=False)
            if not self._df_usages.empty:
                self._df_usages.to_parquet(CACHE_USAGES, index=False)
        except Exception as e:
            logger.error(f"Erreur sauvegarde cache E-Phy: {e}")

    def _load_cache(self):
        try:
            if os.path.exists(CACHE_PRODUITS):
                self._df_produits = pd.read_parquet(CACHE_PRODUITS)
            if os.path.exists(CACHE_USAGES):
                self._df_usages = pd.read_parquet(CACHE_USAGES)
        except Exception as e:
            logger.error(f"Erreur chargement cache E-Phy: {e}")

    # ------------------------------------------------------------------
    # 4. RECHERCHE PAR NOM COMMERCIAL
    # ------------------------------------------------------------------

    def search(self, nom_commercial: str, top_n: int = 5) -> list[dict]:
        """
        Recherche floue par nom commercial dans le référentiel E-Phy.
        Retourne une liste de résultats triés par pertinence.
        Chaque résultat contient :
          - 'intrant'  : dict pour REF_INTRANTS (1 ligne)
          - 'usages'   : list[dict] pour REF_USAGES_PHYTO (N lignes)
          - 'score'    : score de similarité (0-100)
        """
        if self._df_produits.empty:
            return []

        noms = []
        idx_mapping = []
        for idx, row in self._df_produits.iterrows():
            main_nom = str(row.get("Nom_Produit", ""))
            if main_nom and main_nom.lower() not in ("nan", "none", ""):
                noms.append(main_nom)
                idx_mapping.append(idx)
            
            sec_noms = str(row.get("Noms_Secondaires", ""))
            if sec_noms and sec_noms.lower() not in ("nan", "none", ""):
                for sec in sec_noms.split("|"):
                    sec = sec.strip()
                    if sec:
                        noms.append(sec)
                        idx_mapping.append(idx)

        if not noms:
            return []

        matches = process.extract(
            nom_commercial.upper(),
            [n.upper() for n in noms],
            scorer=fuzz.WRatio,
            limit=top_n * 2
        )

        results = []
        seen_amm = set()
        for match_str, score, list_idx in matches:
            if score < 40:
                continue
            
            orig_str = noms[list_idx]
            idx = idx_mapping[list_idx]
            row = self._df_produits.iloc[idx]
            amm = str(row.get("N_AMM", ""))

            if amm in seen_amm:
                continue
            seen_amm.add(amm)

            intrant = row.to_dict()
            intrant.pop("Noms_Secondaires", None)  # Ne pas écrire ça dans REF_INTRANTS
            
            # Si le nom trouvé est un nom secondaire (ex: SPECTRUM au lieu de ISARD),
            # on remplace le Nom_Produit pour qu'il s'affiche et s'enregistre sous ce nom.
            main_nom = str(row.get("Nom_Produit", ""))
            if orig_str.upper() != main_nom.upper():
                intrant["Nom_Produit"] = f"{orig_str} (Réf: {main_nom})"

            # Nettoyer les NaN
            intrant = {k: ("" if (v != v or v is None) else v) for k, v in intrant.items()}

            # Usages associés
            usages = []
            if not self._df_usages.empty and amm:
                sub = self._df_usages[self._df_usages["N_AMM"].astype(str) == amm]
                usages = [{k: ("" if (v != v or v is None) else v) for k, v in r.items()}
                          for r in sub.to_dict("records")]

            results.append({
                "intrant": intrant,
                "usages":  usages,
                "score":   score,
            })
            if len(results) >= top_n:
                break

        return results

    def get_usages_for_product(self, n_amm: str) -> list[dict]:
        """Retourne tous les usages E-Phy pour un N_AMM donné."""
        if self._df_usages.empty:
            return []
        sub = self._df_usages[self._df_usages["N_AMM"].astype(str) == str(n_amm)]
        return [{k: ("" if (v != v or v is None) else v) for k, v in r.items()}
                for r in sub.to_dict("records")]

    @property
    def last_update(self) -> str:
        """Retourne la date de dernière mise à jour du cache (str dd/mm/yyyy)."""
        try:
            with open(CACHE_DATE_FILE, "r") as f:
                d = datetime.strptime(f.read().strip(), "%Y-%m-%d")
            return d.strftime("%d/%m/%Y")
        except Exception:
            return "Inconnue"

    @property
    def nb_produits(self) -> int:
        return len(self._df_produits)

    # ------------------------------------------------------------------
    # 5. UTILITAIRES
    # ------------------------------------------------------------------

    @staticmethod
    def _val(row, col):
        if col is None or col not in row.index:
            return None
        v = row[col]
        if v != v or v is None:  # NaN check
            return None
        v = str(v).strip()
        return v if v not in ("", "nan", "NaN", "None") else None

    @staticmethod
    def _parse_date(val) -> str | None:
        if not val:
            return None
        for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"]:
            try:
                return datetime.strptime(str(val).strip(), fmt).strftime("%d/%m/%Y")
            except Exception:
                continue
        return str(val).strip()

    @staticmethod
    def _build_ephy_link(nom: str) -> str | None:
        if not nom:
            return None
        nom_clean = re.sub(r"[^a-zA-Z0-9]", "-", str(nom).lower())
        nom_clean = re.sub(r"-+", "-", nom_clean).strip("-")
        if nom_clean:
            return f"https://ephy.anses.fr/ppp/{nom_clean}"
        return None

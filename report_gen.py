from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from datetime import datetime
import os
import pandas as pd

class ReportGenerator:
    def __init__(self, filename):
        self.filename = filename
        self.doc = SimpleDocTemplate(filename, pagesize=A4) # Portrait
        self.elements = []
        self.styles = getSampleStyleSheet()
        
        # --- LOGO INTEGRATION ---
        # --- LOGO INTEGRATION ---
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cwd = os.getcwd()
        search_dirs = [base_dir, cwd]
        
        logo_path = None
        
        # Robust case-insensitive search
        for d in search_dirs:
            if not os.path.exists(d): continue
            try:
                # Scan directory for any file matching 'logo.*' (case insensitive)
                for entry in os.scandir(d):
                    if entry.is_file():
                        fname_lower = entry.name.lower()
                        if fname_lower in ['logo.png', 'logo.jpg', 'logo.jpeg']:
                            logo_path = entry.path
                            break
                if logo_path: break
            except Exception as e:
                print(f"Error scanning directory {d}: {e}")
            
        print(f"Logo search paths: {search_dirs}. Found: {logo_path}")
        
        if logo_path:
            try:
                # Add Logo (Width 5cm, conserve aspect ratio)
                im = Image(logo_path)
                desired_width = 5 * cm
                aspect = im.imageHeight / im.imageWidth
                im.drawWidth = desired_width
                im.drawHeight = desired_width * aspect
                im.hAlign = 'LEFT'
                self.elements.append(im)
                self.elements.append(Spacer(1, 15))
            except Exception as e:
                print(f"Warning: Could not load logo: {e}")
        
    def add_title(self, text):
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=16,
            spaceAfter=30,
            alignment=1 # Center
        )
        self.elements.append(Paragraph(text, title_style))

    def add_paragraph(self, text, style_name='Normal'):
        self.elements.append(Paragraph(text, self.styles[style_name]))
        self.elements.append(Spacer(1, 12))

    def generate_phyto_register(self, campaign, data_grouped):
        """
        Generates the Phyto Register.
        data_grouped: dictionary { 'Parcelle_Name': [list of intervention rows] }
        """
        self.doc.pagesize = A4 # Portrait
        self.add_title(f"Registre Phytosanitaire - Campagne {campaign}")
        
        if not data_grouped:
            self.add_paragraph("Aucune intervention phytosanitaire trouvée pour cette campagne.")
        
        for parcelle, data_bundle in data_grouped.items():
            # Unpack data
            interventions = data_bundle.get('data', [])
            meta = data_bundle.get('meta', {})
            
            # --- Header: Parcel Info ---
            # Format: Parcelle (Ilot) - Culture - Surface
            header_text = f"<b>Parcelle : {parcelle}</b>"
            if meta.get('Ilot_PAC', 'N/A') != 'N/A':
                header_text += f" (Ilot: {meta.get('Ilot_PAC')})"
            
            sub_header = f"Culture: {meta.get('Culture', 'N/A')} | Surface: {meta.get('Surface', 'N/A')} ha | Précédent: {meta.get('Precedent', 'N/A')}"
            
            self.elements.append(Paragraph(header_text, self.styles['Heading2']))
            self.elements.append(Paragraph(sub_header, self.styles['Normal']))
            self.elements.append(Spacer(1, 10))
            
            # Table Data Preparation
            # Columns: Date, Culture, Produit, Dose, Unité, Surf., Cible, Obs
            table_data = [['Date', 'Culture', 'Produit', 'Dose/ha', 'Unité', 'Surf.', 'Cible', 'Observations']]
            
            for row in interventions:
                # Format Date
                d_val = row['Date']
                if d_val and hasattr(d_val, 'strftime'):
                    date_str = d_val.strftime('%d/%m/%Y')
                else:
                    date_str = str(d_val) if not pd.isnull(d_val) else ""
                
                # Handle potential NaN
                produit = str(row['Nom_Produit']) if not pd.isnull(row['Nom_Produit']) else ""
                dose = f"{row['Dose_Ha']}" if not pd.isnull(row['Dose_Ha']) else ""
                unite = str(row.get('Unité_Dose', '')) if not pd.isnull(row.get('Unité_Dose', '')) else ""
                
                surf = f"{row['Surface_Travaillée_Ha']}" if not pd.isnull(row['Surface_Travaillée_Ha']) else ""
                cible = str(row['Cible']) if not pd.isnull(row['Cible']) else ""
                obs = str(row['Observations']) if not pd.isnull(row['Observations']) else ""
                culture = str(row['Culture']) if not pd.isnull(row['Culture']) else ""

                table_data.append([date_str, culture, produit, dose, unite, surf, cible, obs])
            
            if len(table_data) > 1: # Only add table if there are rows
                # Table Style
                # Reduced Widths for Portrait (Total ~18cm)
                t = Table(table_data, colWidths=[2.0*cm, 2.5*cm, 3.5*cm, 1.5*cm, 1.2*cm, 1.3*cm, 2.5*cm, 3.5*cm])
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#e0e0e0')),
                    ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0,0), (-1,0), 9), # Smaller Font
                    ('BOTTOMPADDING', (0,0), (-1,0), 12),
                    ('BACKGROUND', (0,1), (-1,-1), colors.white),
                    ('GRID', (0,0), (-1,-1), 1, colors.black),
                    ('FONTSIZE', (0,1), (-1,-1), 7), # Smaller Content Font
                ]))
                self.elements.append(t)
                self.elements.append(Spacer(1, 20))
            else:
                 self.elements.append(Paragraph("<i>Aucune intervention recensée.</i>", self.styles['Normal']))
                 self.elements.append(Spacer(1, 10))

        self.doc.build(self.elements)
        print(f"PDF Generated: {self.filename}")

    def generate_ferti_balance(self, campaign, data_grouped):
        """
        Generates the Fertilization Balance.
        data_grouped: dictionary { 
            'Parcelle_Name': {
                'Apports': [list of dicts],
                'Besoins': {dict of needs},
                'Sol': {dict of soil analysis}
            }
        }
        """
        self.doc.pagesize = A4 # Portrait
        
        # New Page for Ferti if appending to same doc, but here we likely create a new doc or same doc
        # If same doc object is reused, we might need a PageBreak.
        # But we will instantiate a new ReportGenerator for this report usually.
        # Or we can add a method to clear/reset? For now, assume fresh generator or appended.
        
        self.add_title(f"Bilan de Fertilisation - Campagne {campaign}")

        if not data_grouped:
             self.add_paragraph("Aucune données de fertilisation trouvées.")

        for parcelle, data in data_grouped.items():
            apports = data.get('Apports', [])
            besoins = data.get('Besoins', {})
            sol = data.get('Sol', {})
            meta = data.get('meta', {}) # Parcel Metadata

            # --- Header: Parcelle Info + Soil Analysis ---
            header_text = f"<b>Parcelle : {parcelle}</b>"
            if meta.get('Ilot_PAC', 'N/A') != 'N/A':
                header_text += f" (Ilot: {meta.get('Ilot_PAC')})"
            
            # Combine Soil info with general info or keep separate? 
            # User asked for: Campagne, Nom, Culture, Ilot, Surface, Precedent
            sub_header = f"Culture: {meta.get('Culture', 'N/A')} | Surface: {meta.get('Surface', 'N/A')} ha | Précédent: {meta.get('Precedent', 'N/A')}"
            
            self.elements.append(Paragraph(header_text, self.styles['Heading2']))
            self.elements.append(Paragraph(sub_header, self.styles['Normal']))
            
            # Soil Analysis Text (Keep specific to Ferti)
            sol_text = f"<b>Analyse de Sol:</b> Reliquat Hiver: {sol.get('Reliquat', 'N/A')} | Minéralisation Humus: {sol.get('Humus', 'N/A')}"
            self.elements.append(Paragraph(sol_text, self.styles['Normal']))
            self.elements.append(Spacer(1, 10))
            self.elements.append(Spacer(1, 10))

            # --- Balance Calculation (Simplified) ---
            # Needs
            besoin_n = besoins.get('Besoin_N', 0)
            besoin_p = besoins.get('Besoin_P', 0)
            besoin_k = besoins.get('Besoin_K', 0)
            
            # Total Inputs
            # Helper to safely sum
            def clean_float(val):
                try: return float(val)
                except: return 0.0

            total_n = sum([clean_float(x.get('N/ha', 0)) for x in apports])
            total_p = sum([clean_float(x.get('P/ha', 0)) for x in apports])
            total_k = sum([clean_float(x.get('K/ha', 0)) for x in apports])
            
            # Balance
            solde_n = total_n - besoin_n + float(sol.get('Reliquat', 0) or 0) # Simplistic formula
            # Note: Real formula is more complex (Needs - (Soil + Input) = Balance), usually Balance = Inputs - (Needs - SoilSupplies)
            # Let's display Inputs vs Needs table
            
            # --- Table: Inputs ---
            if apports:
                table_data = [['Date', 'Produit', 'Dose/ha', 'Unité', 'N / ha', 'P / ha', 'K / ha']]
                for row in apports:
                    d_val = row['Date']
                    if d_val and hasattr(d_val, 'strftime'):
                        date_str = d_val.strftime('%d/%m/%Y')
                    else:
                        date_str = str(d_val) if not pd.isnull(d_val) else ""
                    table_data.append([
                        date_str,
                        str(row.get('Nom_Produit', '')),
                        str(row.get('Dose_Ha', '')),
                        str(row.get('Unité_Dose', '')),
                        str(row.get('N/ha', '')),
                        str(row.get('P/ha', '')),
                        str(row.get('K/ha', ''))
                    ])
                
                # Summary Row (Sum logic needs adjustment for N/ha keys or keep total logic separate)
                # Recalculate Totals based on correct keys 'N/ha'
                def clean_float(val):
                    try: return float(val)
                    except: return 0.0
                    
                total_n = sum([clean_float(x.get('N/ha', 0)) for x in apports])
                total_p = sum([clean_float(x.get('P/ha', 0)) for x in apports])
                total_k = sum([clean_float(x.get('K/ha', 0)) for x in apports])

                table_data.append(['TOTAL', '', '', '', f"{total_n:.1f}", f"{total_p:.1f}", f"{total_k:.1f}"])
                
                # Reduced Widths (~18cm)
                t = Table(table_data, colWidths=[2.5*cm, 5.5*cm, 1.5*cm, 1.5*cm, 1.8*cm, 1.8*cm, 1.8*cm])
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#d1e7dd')), # Greenish for Ferti
                    ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0,0), (-1,0), 9),
                    ('GRID', (0,0), (-1,-1), 1, colors.black),
                    ('FONTNAME', (0,-1), (-1,-1), 'Helvetica-Bold'), # Bold Total
                ]))
                self.elements.append(t)
            else:
                 self.elements.append(Paragraph("<i>Aucun apport enregistré.</i>", self.styles['Normal']))
            
            self.elements.append(Spacer(1, 10))
            
            # --- Summary/Balance Section ---
            balance_text = f"<b>Bilan Prévisionnel NPK:</b><br/>" \
                           f"Besoins: N={besoin_n}, P={besoin_p}, K={besoin_k}<br/>" \
                           f"Apports Totaux: N={total_n:.1f}, P={total_p:.1f}, K={total_k:.1f}"
            self.elements.append(Paragraph(balance_text, self.styles['Normal']))
            
            self.elements.append(Spacer(1, 20))
            
        self.doc.build(self.elements)
        print(f"PDF Generated: {self.filename}")

    def generate_itk(self, campaign, data_grouped):
        """
        Generates the Itinéraire Technique (ITK) Report.
        Data structure expected:
        data_grouped[parcelle] = {
            'meta': {...},
            'Travail du sol': [...],
            'Semis': [...],
            'Fertilisation': [...],
            'Traitement': [...],
            'Récolte': [...]
        }
        """
        self.doc.pagesize = A4 # Portrait
        self.add_title(f"Itinéraire Technique - Campagne {campaign}")

        if not data_grouped:
            self.add_paragraph("Aucune donnée disponible pour l'itinéraire technique.")

        for parcelle, content in data_grouped.items():
            meta = content.get('meta', {})
            
            # --- Header ITK ---
            header_text = f"<b>Parcelle : {parcelle}</b>"
            if meta.get('Ilot_PAC', 'N/A') != 'N/A':
                header_text += f" (Ilot: {meta.get('Ilot_PAC')})"
            
            sub_header = f"Culture: {meta.get('Culture', 'N/A')} | Surface: {meta.get('Surface', 'N/A')} ha | Précédent: {meta.get('Precedent', 'N/A')}"
            if meta.get('Variete'):
                 sub_header += f" | Variété: {meta.get('Variete')}"

            self.elements.append(Paragraph(header_text, self.styles['Heading2']))
            self.elements.append(Paragraph(sub_header, self.styles['Normal']))
            self.elements.append(Spacer(1, 10))
            
            # --- Helper to add section table ---
            def add_section_table(title, rows, headers, col_widths, map_func):
                if not rows: return
                
                self.elements.append(Paragraph(f"<b>{title}</b>", self.styles['Heading3']))
                
                table_data = [headers]
                for r in rows:
                    table_data.append(map_func(r))
                
                t = Table(table_data, colWidths=col_widths)
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                    ('TEXTCOLOR', (0,0), (-1,0), colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0,0), (-1,0), 9),
                    ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                ]))
                self.elements.append(t)
                self.elements.append(Spacer(1, 10))

            # 1. Travail du Sol
            # Cols: Date, Nature, Outil, Obs
            def map_sol(r):
                d_val = r.get('Date')
                if d_val and hasattr(d_val, 'strftime'):
                    d = d_val.strftime('%d/%m/%Y')
                else:
                    d = str(d_val) if not pd.isnull(d_val) else ""
                nature = str(r.get('Nature_Intervention', ''))
                outil = str(r.get('Outil', '') or r.get('Nom_Produit', '') or r.get('Type_Intervention', ''))
                obs = str(r.get('Observations', ''))
                return [d, nature, outil, obs]

            add_section_table(
                "Travail du Sol",
                content.get('Travail du sol', []),
                ['Date', 'Intervention', 'Outil', 'Observations'],
                [2.5*cm, 4*cm, 4*cm, 7.5*cm],
                map_sol
            )

            # 2. Semis
            # Cols: Date, Produit, Dose, Unité, Obs
            def map_semi(r):
                d_val = r.get('Date')
                if d_val and hasattr(d_val, 'strftime'):
                    d = d_val.strftime('%d/%m/%Y')
                else:
                    d = str(d_val) if not pd.isnull(d_val) else ""
                prod = str(r.get('Nom_Produit', '')) 
                # User requested Dose/Unité. Prefer Dose_Ha, fallback to Densité if Dose_Ha is empty/0
                dose_val = r.get('Dose_Ha', '')
                if not dose_val and dose_val != 0:
                     dose_val = r.get('Densité_Semis', '')
                dose = f"{dose_val}"
                
                unit = str(r.get('Unité_Dose', '') or r.get('Unité_Densité', ''))
                obs = str(r.get('Observations', ''))
                return [d, prod, dose, unit, obs]

            add_section_table(
                "Semis",
                content.get('Semis', []),
                ['Date', 'Produit', 'Dose', 'Unité', 'Observations'],
                [2.5*cm, 5*cm, 1.5*cm, 1.5*cm, 7.5*cm],
                map_semi
            )

            # 3. Fertilisation
            # Cols: Date, Engrais, Dose, Unité, N, P, K
            def map_ferti(r):
                d_val = r.get('Date')
                if d_val and hasattr(d_val, 'strftime'):
                    d = d_val.strftime('%d/%m/%Y')
                else:
                    d = str(d_val) if not pd.isnull(d_val) else ""
                prod = str(r.get('Nom_Produit', ''))
                dose = f"{r.get('Dose_Ha', '')}"
                unit = str(r.get('Unité_Dose', ''))
                n = f"{r.get('N/ha', '')}"
                p = f"{r.get('P/ha', '')}"
                k = f"{r.get('K/ha', '')}"
                return [d, prod, dose, unit, n, p, k]
            
            add_section_table(
                "Fertilisation",
                content.get('Fertilisation', []),
                ['Date', 'Engrais', 'Dose', 'Unité', 'N', 'P', 'K'],
                [2.5*cm, 5.5*cm, 1.5*cm, 1.5*cm, 1.8*cm, 1.8*cm, 1.8*cm],
                map_ferti
            )

            # 4. Traitement (Phyto)
            # Cols: Date, Produit, Dose, Unité, Cible, Obs
            def map_phyto(r):
                d_val = r.get('Date')
                if d_val and hasattr(d_val, 'strftime'):
                    d = d_val.strftime('%d/%m/%Y')
                else:
                    d = str(d_val) if not pd.isnull(d_val) else ""
                prod = str(r.get('Nom_Produit', ''))
                dose = f"{r.get('Dose_Ha', '')}"
                unit = str(r.get('Unité_Dose', ''))
                cible = str(r.get('Cible', ''))
                obs = str(r.get('Observations', ''))
                return [d, prod, dose, unit, cible, obs]
            
            # Grouping Logic for Phyto
            raw_treatments = content.get('Traitement', [])
            grouped_treatments = []
            treatments_by_date = {}
            
            for t in raw_treatments:
                d_val = t.get('Date')
                if pd.isnull(d_val):
                    key = "Inconnue"
                else:
                    key = d_val.strftime('%Y-%m-%d')
                
                if key not in treatments_by_date:
                    treatments_by_date[key] = []
                treatments_by_date[key].append(t)
            
            for key in sorted(treatments_by_date.keys()):
                group = treatments_by_date[key]
                first = group[0]
                
                prods = [str(x.get('Nom_Produit', '')) for x in group]
                doses = [str(x.get('Dose_Ha', '')) for x in group]
                units = [str(x.get('Unité_Dose', '')) for x in group]
                
                cibles = []
                for x in group:
                    c = str(x.get('Cible', '')).strip()
                    if c and c not in cibles: cibles.append(c)
                
                obs = []
                for x in group:
                    o = str(x.get('Observations', '')).strip()
                    if o and o not in obs: obs.append(o)
                
                combined = {
                    'Date': first['Date'],
                    'Nom_Produit': '\n'.join(prods),
                    'Dose_Ha': '\n'.join(doses),
                    'Unité_Dose': '\n'.join(units),
                    'Cible': '\n'.join(cibles),
                    'Observations': '\n'.join(obs)
                }
                grouped_treatments.append(combined)

            add_section_table(
                "Protection des Plantes (Phyto)",
                grouped_treatments,
                ['Date', 'Produit', 'Dose', 'Unité', 'Cible', 'Observations'],
                [2.5*cm, 4*cm, 1.5*cm, 1.5*cm, 3.5*cm, 5*cm],
                map_phyto
            )

            # 5. Récolte
            # Cols: Date, Rendement, Humidité, Obs
            def map_recolte(r):
                d_val = r.get('Date')
                if d_val and hasattr(d_val, 'strftime'):
                    d = d_val.strftime('%d/%m/%Y')
                else:
                    d = str(d_val) if not pd.isnull(d_val) else ""
                rend = str(r.get('Rendement_Ha', '') or r.get('Quantité_Récoltée_Totale', ''))
                hum = str(r.get('Humidité_récolte', ''))
                obs = str(r.get('Observations', ''))
                return [d, rend, hum, obs]

            add_section_table(
                "Récolte",
                content.get('Récolte', []),
                ['Date', 'Rendement (q/ha)', 'Humidité (%)', 'Observations'],
                [2.5*cm, 3.5*cm, 3.5*cm, 8.5*cm],
                map_recolte
            )

            self.elements.append(Spacer(1, 20)) 

        self.doc.build(self.elements)
        print(f"PDF Generated: {self.filename}")


    def generate_prep_sheet(self, campaign, intervention_data):
        """
        Generates the Phyto Preparation Sheet (Fiche de Préparation de Bouillie).
        intervention_data: dict containing:
            'Parcelle': str
            'Surface': float
            'Date': datetime
            'Volume_Bouillie_Ha': float (optional, default 150)
            'Products': list of dicts (Nom_Produit, Dose_Ha, Formulation, etc.) sorted by mixing order
            'Intervention_ID': str (unique ID for QR)
        """
        self.doc.pagesize = A4 # Portrait
        
        # --- Header ---
        parcelle = intervention_data.get('Parcelle', 'Inconnue')
        surface = float(intervention_data.get('Surface', 0))
        date_prevue = intervention_data.get('Date')
        if date_prevue and hasattr(date_prevue, 'strftime'):
            date_str = date_prevue.strftime('%d/%m/%Y')
        else:
            date_str = str(date_prevue) if date_prevue else "Non définie"
        
        self.add_title(f"Fiche de Préparation Phyto - {date_str}")
        
        # --- Parcelle Info & Volume ---
        vol_ha = float(intervention_data.get('Volume_Bouillie_Ha', 100)) # Default 100L/ha if missing
        vol_total = surface * vol_ha
        
        header_table_data = [
            [f"PARCELLE : {parcelle}", f"SURFACE : {surface:.2f} ha"],
            [f"Volume Bouillie / ha : {vol_ha:.0f} L/ha", f"VOLUME TOTAL CUVE : {vol_total:.0f} Litres"]
        ]
        
        t_header = Table(header_table_data, colWidths=[9*cm, 9*cm])
        t_header.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), colors.lightgrey),
            ('TEXTCOLOR', (0,0), (-1,-1), colors.black),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,-1), 12),
            ('GRID', (0,0), (-1,-1), 1, colors.black),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
            ('TOPPADDING', (0,0), (-1,-1), 10),
        ]))
        self.elements.append(t_header)
        self.elements.append(Spacer(1, 15))
        
        # --- Sécurité (EPI) ---
        # Text based icons for robustness
        epi_text = "⚠️ SÉCURITÉ / EPI OBLIGATOIRES ⚠️"
        epi_details = "🧤 Gants Nitrile   😷 Masque (A2P3)   🥽 Lunettes   🥼 Combinaison"
        
        self.elements.append(Paragraph(epi_text, self.styles['Heading2']))
        self.elements.append(Paragraph(epi_details, ParagraphStyle('EPI', parent=self.styles['Normal'], fontSize=12, alignment=1, textColor=colors.red)))
        self.elements.append(Spacer(1, 15))
        
        # --- Checklist Produits (Mixing Order) ---
        self.elements.append(Paragraph("<b>🛠️ ORDRE D'INCORPORATION & DOSAGES</b>", self.styles['Heading3']))
        
        # Columns: [ ] Check, Ordre, Produit, Formulation, Dose/ha, Qté Totale
        table_data = [['OK', 'Ordre', 'Produit', 'Formulation', 'Dose/ha', 'Qté TOTALE']]
        
        products = intervention_data.get('Products', [])
        
        for idx, prod in enumerate(products, 1):
            p_name = prod.get('Nom_Produit', 'Inconnu')
            form = prod.get('Formulation', '-')
            dose_ha = float(prod.get('Dose_Ha', 0))
            unite = prod.get('Unité_Dose', '')
            
            qty_total = dose_ha * surface
            
            # Format numbers
            dose_str = f"{dose_ha} {unite}"
            qty_str = f"{qty_total:.2f} {unite}"
            
            # Checkbox placeholder (Empty square)
            checkbox = "⬜" 
            
            table_data.append([checkbox, str(idx), p_name, form, dose_str, qty_str])
            
        t_prods = Table(table_data, colWidths=[1.5*cm, 1.5*cm, 6*cm, 2.5*cm, 3*cm, 3.5*cm])
        t_prods.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#2E7D32')), # Agri Green
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 10),
            ('GRID', (0,0), (-1,-1), 1, colors.black),
            ('FONTSIZE', (0,1), (-1,-1), 11),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('TOPPADDING', (0,0), (-1,-1), 8),
            # Bold Total Quantity
            ('FONTNAME', (-1,1), (-1,-1), 'Helvetica-Bold'),
        ]))
        self.elements.append(t_prods)
        self.elements.append(Spacer(1, 25))
        
        # --- QR Code & Validation ---
        import qrcode
        from reportlab.lib.utils import ImageReader
        import io
        
        intervention_id = intervention_data.get('Intervention_ID', 'test')
        
        qr_payload = f"https://share.streamlit.io/?validate_phyto={intervention_id}" 
        # Note: Ideally we want the specific app url. We'll use a generic text for now.
        
        qr = qrcode.QRCode(box_size=10, border=4)
        qr.add_data(qr_payload)
        qr.make(fit=True)
        img_qr = qr.make_image(fill='black', back_color='white')
        
        # Convert to ReportLab Image
        img_buffer = io.BytesIO()
        img_qr.save(img_buffer, format='PNG')
        img_buffer.seek(0)
        
        # Draw QR
        im = Image(img_buffer, width=4*cm, height=4*cm)
        self.elements.append(im)
        self.elements.append(Paragraph(f"Scan pour valider : {intervention_id}", self.styles['Normal']))
        
        self.doc.build(self.elements)
        print(f"PDF Generated: {self.filename}")


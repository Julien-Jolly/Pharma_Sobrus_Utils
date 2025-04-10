import re

class PDFProcessor:
    def __init__(self):
        pass

    def extract_sorted_lines(self, pdf_path):
        import pdfplumber
        all_lines = []
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Nombre de pages dans {pdf_path}: {len(pdf.pages)}")
            for page in pdf.pages:
                words = page.extract_words()
                line_map = {}
                for word in words:
                    top = round(word['top'])
                    if top not in line_map:
                        line_map[top] = []
                    line_map[top].append(word['text'])
                for top in sorted(line_map.keys()):
                    line = " ".join(line_map[top])
                    all_lines.append(line)
        print(f"Lignes extraites de {pdf_path}: {len(all_lines)}")
        if all_lines:
            print(f"Premières lignes extraites: {all_lines[:5]}")
        return all_lines

    def parse_line(self, line):
        """
        Extrait date, libellé, total, solde d'une ligne simple de type :
        2024-03-27 - Paiement vente 84,91 -1 929,9
        """
        match = re.match(
            r"^(\d{4}-\d{2}-\d{2})\s+(.*?)\s+(-?\d{1,3}(?:[\.,]\d{2}))\s+(-?\d{1,3}(?:[\s.,]\d{3})*[\.,]\d{2})$",
            line.strip()
        )
        if match:
            date, libelle, total_str, solde_str = match.groups()
            total = float(total_str.replace(",", "."))
            solde = float(solde_str.replace(" ", "").replace(",", "."))
            return {
                "date": date,
                "reference": None,
                "libelle": libelle.strip(),
                "total_brut": total,
                "solde_lu": solde
            }
        return None

    def extract_detailed_data(self, pdf_file, client):
        import pdfplumber

        def parse_line(line):
            match = re.match(
                r"^(\d{4}-\d{2}-\d{2})\s+(.*?)\s+(-?\d{1,3}(?:[\.,]\d{2}))\s+(-?\d{1,3}(?:[\s.,]\d{3})*[\.,]\d{2})$",
                line.strip()
            )
            if match:
                date, libelle, total_str, solde_str = match.groups()
                total = float(total_str.replace(",", "."))
                solde = float(solde_str.replace(" ", "").replace(",", "."))
                return {
                    "date": date,
                    "reference": None,
                    "libelle": libelle.strip().removeprefix("- ").strip(),
                    "total_brut": total,
                    "solde_lu": solde
                }
            return None

        records = []
        solde = 0.0
        solde_final_pdf = None
        raw_rows = []

        # Lire le contenu du PDF
        lines = self.extract_sorted_lines(pdf_file)

        start_parsing = False
        for i, line in enumerate(lines):
            line = line.strip()

            if "solde initial" in line.lower():
                match = re.search(r"(\d+[ ,]?\d{0,3}(?:[.,]\d+))", line)
                if match:
                    solde = float(match.group(1).replace(" ", "").replace(",", "."))
                    print(f"Solde initial détecté : {solde:.2f}")

            if re.match(r"^Date\s+Transaction\s+N°\s+Libellé\s+Total\s+Solde", line, re.IGNORECASE):
                start_parsing = True
                continue

            if "solde final" in line.lower():
                match = re.search(r"(\d+[ ,]?\d{0,3}(?:[.,]\d+))", line)
                if match:
                    solde_final_pdf = float(match.group(1).replace(" ", "").replace(",", "."))
                    print(f"Solde final indiqué dans le PDF : {solde_final_pdf:.2f}")
                break

            if start_parsing and re.match(r"^\d{4}-\d{2}-\d{2}", line):
                parsed = parse_line(line)
                if parsed:
                    parsed["nom"] = client["nom"]
                    raw_rows.append(parsed)

        # Étape 1 : regrouper les paiements par date
        paiements_par_date = {}
        for row in raw_rows:
            if "paiement" in row["libelle"].lower():
                date = row["date"]
                montant = abs(row["total_brut"])
                paiements_par_date.setdefault(date, []).append(montant)

        # Étape 2 : identifier les dates où paiements = retour
        paiements_a_ignorer = set()
        for row in raw_rows:
            if "retour" in row["libelle"].lower():
                date = row["date"]
                montant_retour = abs(row["total_brut"])
                paiements_du_jour = paiements_par_date.get(date, [])
                if abs(sum(paiements_du_jour) - montant_retour) < 0.01:
                    paiements_a_ignorer.add(date)

        # Retours et avoirs pour les règles précédentes
        retours = {(r["date"], abs(r["total_brut"])) for r in raw_rows if "retour" in r["libelle"].lower()}
        avoirs = {(r["date"], abs(r["total_brut"])) for r in raw_rows if "avoir" in r["libelle"].lower()}

        for row in raw_rows:
            date = row["date"]
            reference = row.get("reference")
            libelle = row["libelle"]
            type_ligne = libelle.lower()
            brut = abs(row["total_brut"])

            if "paiement" in type_ligne:
                montant = -brut
            elif "retour" in type_ligne:
                montant = -brut
            elif "avoir" in type_ligne:
                montant = -brut
            else:
                montant = brut

            appliquer = True
            if "paiement" in type_ligne and (
                    (date, brut) in retours or (date, brut) in avoirs or date in paiements_a_ignorer):
                appliquer = False

            effet = montant if appliquer else 0.0
            solde = round(solde + effet, 2)

            records.append({
                "nom": client["nom"],
                "date": date,
                "reference": reference,
                "libelle": libelle,
                "total": montant,
                "solde": solde
            })

        if solde_final_pdf is not None and abs(solde - solde_final_pdf) > 0.01:
            print(f"[AVERTISSEMENT] Solde final recalculé ({solde:.2f}) != PDF ({solde_final_pdf:.2f})")

        return records, solde_final_pdf


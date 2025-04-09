import pdfplumber
import re
import os

def extract_sorted_lines(pdf_path):
    all_lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            words = page.extract_words()
            line_map = {}
            for word in words:
                top = round(word['top'])  # group by Y position
                if top not in line_map:
                    line_map[top] = []
                line_map[top].append(word['text'])
            for top in sorted(line_map.keys()):
                line = " ".join(line_map[top])
                all_lines.append(line)
    return all_lines

def parse_pdf(pdf_path):
    if not os.path.exists(pdf_path):
        print(f"Fichier non trouvé : {pdf_path}")
        return

    lines = extract_sorted_lines(pdf_path)
    solde = 0.0
    i = 0
    current_transaction = "Unknown"

    print("---- Résultat Extraction ----")

    while i < len(lines):
        line = lines[i].strip()

        # Détection des ventes ou retours
        vente_match = re.match(r"^(\d{4}-\d{2}-\d{2})\s+((FAC|RV)-\d+)\s+(.+)", line)
        if vente_match:
            date, transaction_num, transaction_type, libelle = vente_match.groups()
            current_transaction = transaction_num
            is_return = transaction_type.startswith("RV")
            print(f"{date} | {transaction_num} | {'Retour' if is_return else 'Vente'} {solde:.2f}")
            i += 1

            # Lignes produits
            while i < len(lines):
                prod_line = lines[i].strip()
                if re.match(r"^Total\s+\d", prod_line, re.IGNORECASE):
                    tokens = prod_line.split()
                    total = float(tokens[1].replace(",", "."))
                    solde = solde - total if is_return else solde + total
                    print(f"           |            | Total {total:.2f} {solde:.2f}")
                    i += 1
                    break
                # Extraire tous les nombres de la ligne
                numbers = re.findall(r"[\d,]+", prod_line)
                if len(numbers) >= 4:
                    # Si 5 nombres (ex. 1 48,00 4,80 43,20 0)
                    if len(numbers) >= 5:
                        quantite = int(numbers[-5])
                        pu = float(numbers[-4].replace(",", "."))
                        remise = float(numbers[-3].replace(",", "."))
                        pu_remise = float(numbers[-2].replace(",", "."))
                    # Si 4 nombres (ex. 6 15,00 15,00 566,19 ou 1 48,00 4,80 43,20)
                    else:
                        quantite = int(numbers[-4])
                        pu = float(numbers[-3].replace(",", "."))
                        remise = float(numbers[-2].replace(",", "."))
                        pu_remise = float(numbers[-1].replace(",", "."))
                        # Pour FAC : si pu = remise (ex. 15,00 15,00), remise = 0
                        if not is_return and abs(pu - remise) < 0.01:
                            remise = 0.0
                            pu_remise = pu
                        # Pour RV : remise = 0 toujours
                        if is_return:
                            remise = 0.0
                            pu_remise = pu if abs(pu - pu_remise) > 0.01 else pu_remise
                    total_calcule = round(quantite * pu_remise, 2)
                    print(f" |  |  |  | {quantite} | {pu:.2f} | {remise:.2f} | {pu_remise:.2f} | {total_calcule:.2f}")
                i += 1
            continue

        # Détection paiement / avoir sur 3 lignes
        if i + 2 < len(lines):
            label1 = lines[i].strip().lower()
            date_and_montant = lines[i + 1].strip()
            label2 = lines[i + 2].strip().lower()

            if label1 in ["paiement", "avoir"] and re.match(r"^\d{4}-\d{2}-\d{2}", date_and_montant) and \
                    label2 in ["vente", "client"]:
                date = date_and_montant.split()[0]
                montant_match = re.search(r"(\d+[.,]\d+|\d+)", date_and_montant.split(date, 1)[1])
                if montant_match:
                    montant = float(montant_match.group(0).replace(",", "."))
                    print(f"{date} | -          | {label1.capitalize()} {label2} |  |  |  |  |  | {montant:.2f} | {solde - montant:.2f}")
                    solde = round(solde - montant, 2)
                i += 3
                continue

        i += 1

if __name__ == "__main__":
    pdf_path = "C:/Users/Julien/Downloads/Sobrus - Relevé de -  AMINA CHERKAOUI.pdf"
    parse_pdf(pdf_path)
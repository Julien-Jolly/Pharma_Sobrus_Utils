import pdfplumber
import re
import os

class PDFProcessor:
    def __init__(self):
        pass

    def extract_sorted_lines(self, pdf_path):
        all_lines = []
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Nombre de pages dans {pdf_path}: {len(pdf.pages)}")
            for page in pdf.pages:
                words = page.extract_words()
                line_map = {}
                for word in words:
                    top = round(word['top'])  # Grouper par position Y
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

    def extract_detailed_data(self, pdf_file, client):
        records = []
        solde = 0.0  # Solde initial
        solde_final = 0.0
        current_transaction = "Unknown"

        print(f"---- Résultat Extraction pour {client['nom']} ----")

        lines = self.extract_sorted_lines(pdf_file)
        print(f"Total lignes à traiter: {len(lines)}")
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            print(f"Ligne {i}: {line}")

            # Détection des ventes ou retours
            vente_match = re.match(r"^(\d{4}-\d{2}-\d{2})\s+((FAC|RV)-\d+)\s+(.+)", line)
            if vente_match:
                date, transaction_num, transaction_type, libelle = vente_match.groups()
                current_transaction = transaction_num
                is_return = transaction_type.startswith("RV")
                print(f"{date} | {transaction_num} | {'Retour' if is_return else 'Vente'} | Solde initial: {solde:.2f}")
                i += 1

                # Lignes produits
                while i < len(lines):
                    prod_line = lines[i].strip()
                    if re.match(r"^Total\s+\d", prod_line, re.IGNORECASE):
                        tokens = prod_line.split()
                        total = float(tokens[1].replace(",", "."))
                        # On ignore le total du PDF, on va recalculer
                        i += 1
                        break
                    # Extraire tous les nombres de la ligne
                    numbers = re.findall(r"[\d,]+(?:\.\d+)?", prod_line)
                    if len(numbers) >= 4:
                        # Créer un enregistrement pour chaque produit
                        record = {
                            "nom": client['nom'],
                            "date": date,
                            "reference": transaction_num,
                            "produit": None,
                            "quantite": None,
                            "prix_unitaire": None,
                            "remise": None,
                            "prix_unitaire_remise": None,
                            "total": None,
                            "solde": None
                        }
                        # Nettoyer les nombres pour gérer les virgules
                        cleaned_numbers = [num.replace(",", ".") for num in numbers]
                        # Si 5 nombres (ex. 1 48,00 4,80 43,20 0)
                        if len(cleaned_numbers) >= 5:
                            try:
                                quantite = int(cleaned_numbers[-5])
                            except ValueError as e:
                                print(f"Erreur lors de la conversion de la quantité : {cleaned_numbers[-5]}")
                                i += 1
                                continue
                            pu = float(cleaned_numbers[-4])
                            remise = float(cleaned_numbers[-3])
                            pu_remise = float(cleaned_numbers[-2])
                            # Ignorer le dernier nombre (total du PDF)
                        # Si 4 nombres (ex. 6 15,00 15,00 566,19 ou 1 48,00 4,80 43,20)
                        else:
                            try:
                                quantite = int(cleaned_numbers[-4])
                            except ValueError as e:
                                print(f"Erreur lors de la conversion de la quantité : {cleaned_numbers[-4]}")
                                i += 1
                                continue
                            pu = float(cleaned_numbers[-3])
                            remise = float(cleaned_numbers[-2])
                            pu_remise = float(cleaned_numbers[-1])
                            # Pour FAC : si pu = remise (ex. 15,00 15,00), remise = 0
                            if not is_return and abs(pu - remise) < 0.01:
                                remise = 0.0
                                pu_remise = pu
                            # Pour RV : remise = 0 toujours
                            if is_return:
                                remise = 0.0
                                pu_remise = pu if abs(pu - pu_remise) > 0.01 else pu_remise
                        # Calculer le total correctement
                        total = round(quantite * pu_remise, 2)
                        # Mettre à jour le solde
                        solde = round(solde + total if not is_return else solde - total, 2)
                        # Revenir en arrière pour trouver le nom du produit
                        j = i - 1
                        produit = ""
                        while j >= 0 and not re.match(r"^\d{4}-\d{2}-\d{2}", lines[j]):
                            if not re.match(r"[\d,]+\s+[\d,]+", lines[j]):  # Éviter les lignes de nombres
                                produit = lines[j].strip()
                                break
                            j -= 1
                        record.update({
                            "produit": produit,
                            "quantite": quantite,
                            "prix_unitaire": pu,
                            "remise": remise,
                            "prix_unitaire_remise": pu_remise,
                            "total": total if not is_return else -total,  # Total négatif pour les retours
                            "solde": solde
                        })
                        records.append(record)
                        print(f" |  |  |  | {quantite} | {pu:.2f} | {remise:.2f} | {pu_remise:.2f} | Total: {record['total']:.2f} | Solde: {solde:.2f}")
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
                        # Paiement ou avoir : soustraire du solde
                        solde = round(solde - montant, 2)
                        # Ajouter un enregistrement pour le paiement/avoir
                        record = {
                            "nom": client['nom'],
                            "date": date,
                            "reference": None,
                            "produit": None,
                            "quantite": None,
                            "prix_unitaire": None,
                            "remise": None,
                            "prix_unitaire_remise": None,
                            "total": -montant,  # Total négatif pour les paiements/avoirs
                            "solde": solde
                        }
                        records.append(record)
                        print(f"{date} | -          | {label1.capitalize()} {label2} |  |  |  |  |  | Total: {-montant:.2f} | Solde: {solde:.2f}")
                    i += 3
                    continue

            # Détection du solde final
            if "solde" in line.lower() and i + 2 < len(lines):
                next_line = lines[i + 1].strip()
                next_next_line = lines[i + 2].strip()
                if "final" in next_next_line.lower():
                    solde_match = re.match(r"([\d,]+\.\d{2})", next_line)
                    if solde_match:
                        solde_final = float(solde_match.group(1).replace(",", "."))
                        print(f"Solde final détecté dans le PDF : {solde_final:.2f}")
                        print(f"Solde calculé : {solde:.2f}")
                        # Vérifier si le solde calculé correspond au solde final du PDF
                        if abs(solde - solde_final) > 0.01:
                            print(f"⚠️ Avertissement : Le solde calculé ({solde:.2f}) ne correspond pas au solde final du PDF ({solde_final:.2f})")
                        i += 3
                        continue
            i += 1

        print(f"Nombre total d'enregistrements extraits: {len(records)}")
        if records:
            print(f"Exemple d'enregistrement: {records[0]}")
        return records, solde_final
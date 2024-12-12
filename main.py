import pandas as pd

# Charger le CSV
csv_file = "ton_fichier.csv"  # Remplace par ton chemin
df = pd.read_csv(csv_file)

# Étape 1 : Remplacer les apostrophes dans les chaînes de caractères
def escape_apostrophes(value):
    if isinstance(value, str):
        return value.replace("'", "''")  # Double apostrophe pour SQL
    return value

df = df.applymap(escape_apostrophes)

# Étape 2 : Remplacer ou supprimer les valeurs indésirables
valeurs_non_acceptees = ["N/A", "Undefined", None]
df.replace(valeurs_non_acceptees, None, inplace=True)  # Remplace par `None` (null)

# Étape 3 : Exporter le fichier propre
output_file = "fichier_nettoye.csv"
df.to_csv(output_file, index=False)

print(f"Fichier nettoyé exporté sous le nom : {output_file}")
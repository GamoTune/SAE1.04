# Importation des bibliothèques
import csv
import sqlite3 as sql

def connexion() -> tuple[sql.Connection, sql.Cursor]:
    """
    Fonction qui permet de se connecter à la base de données vgsales.db.

    Args:
        (None): Cette fonction ne prend pas d'arguments.

    Returns:
        (tuple[sql.Connection, sql.Cursor]): Cette fonction retourne un tuple contenant la connexion et le curseur de la base de données vgsales.db.
    """

    # Déclaration des variables
    conn: sql.Connection
    cursor: sql.Cursor

    # Connexion à la base de données
    conn = sql.connect('vgsales.db')
    cursor = conn.cursor()

    # On retourne la connexion et le curseur
    return conn, cursor



def creation_table(conn: sql.Connection, cursor: sql.Cursor) -> None:
    """
    Fonction qui permet de créer la table jeux_video dans la base de données vgsales.db.

    Args:
        (sql.Connection, sql.Cursor): Cette fonction prend en argument la connexion et le curseur de la base de données vgsales.db.

    Returns:
        (None): Cette fonction ne retourne rien.
    """

    # Création de la table jeux_video
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS editeurs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom VARCHAR(50)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom VARCHAR(50)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plateformes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom VARCHAR(50)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jeux (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom VARCHAR(255),
            plateforme INTEGER,
            annee INTEGER,
            genre INTEGER,
            editeur INTEGER,
            ventes_AN DECIMAL(5,2),
            ventes_UE DECIMAL(5,2),
            ventes_JP DECIMAL(5,2),
            ventes_autres DECIMAL(5,2),
            ventes_global DECIMAL(5,2),
            FOREIGN KEY (plateforme) REFERENCES plateformes(id),
            FOREIGN KEY (genre) REFERENCES genres(id),
            FOREIGN KEY (editeur) REFERENCES editeurs(id)
        )
    """)



    # On commit et on ferme la connexion
    conn.commit()
    conn.close()



def get_ou_ajout_id(cursor: sql.Cursor, table: str, column: str, value: str) -> int | None:
    """
    Récupère l'id correspondant à une valeur dans une table, ou insère la valeur si elle n'existe pas.

    Args:
        cursor (sql.Cursor): Le curseur de la base de données.
        table (str): Le nom de la table.
        column (str): Le nom de la colonne.
        value (str): La valeur à chercher.

    Returns:
        int: L'id de la valeur.
    """

    # Vérifier si la valeur existe déjà
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]

    # Insérer la valeur
    cursor.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))
    return cursor.lastrowid



def ajout_des_valeurs(conn: sql.Connection, cursor: sql.Cursor) -> None:
    """
    Ajoute les valeurs du fichier vgsales.csv dans les tables de la base de données vgsales.db.

    Args:
        conn (sql.Connection): La connexion à la base de données.
        cursor (sql.Cursor): Le curseur de la base de données.

    Returns:
        (None): Cette fonction ne retourne rien.
    """

    # Ouverture du fichier vgsales.csv
    with open('vgsales.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader) # Ignore la ligne d'en-tête

        # Parcourir chaque ligne du fichier
        est_valide: bool = True
        for row in reader:
            est_valide = True
            # Vérifier si la ligne est valide
            for valeur in row:
                if valeur == 'N/A':
                    est_valide = False
            if est_valide:
                nom = row[1]
                plateforme = row[2]
                annee = int(row[3])
                genre = row[4]
                editeur = row[5]
                ventes_AN = round(float(row[6]), 2)
                ventes_UE = round(float(row[7]), 2)
                ventes_JP = round(float(row[8]), 2)
                ventes_autres = round(float(row[9]), 2)
                ventes_global = round(float(row[10]), 2)

                # Obtenir ou insérer les IDs des tables liées
                plateforme_id = get_ou_ajout_id(cursor, 'plateformes', 'nom', plateforme)
                genre_id = get_ou_ajout_id(cursor, 'genres', 'nom', genre)
                editeur_id = get_ou_ajout_id(cursor, 'editeurs', 'nom', editeur)

                # Insérer dans la table jeux
                try:
                    cursor.execute("""
                        INSERT INTO jeux (nom, plateforme, annee, genre, editeur, ventes_AN, ventes_UE, ventes_JP, ventes_autres, ventes_global)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (nom, plateforme_id, annee, genre_id, editeur_id, ventes_AN, ventes_UE, ventes_JP, ventes_autres, ventes_global))
                except Exception as e:
                    print(f"Erreur lors de l'insertion de {row}: {e}")

    conn.commit()



if __name__ == '__main__':
    # Connexion à la base de données
    conn, cursor = connexion()

    # Création de la table jeux_video
    creation_table(conn, cursor)
    
    conn, cursor = connexion()
    # Ajout des valeurs du fichier vgsales.csv dans la table jeux_video
    ajout_des_valeurs(conn, cursor)
# Importation des bibliothèques
import csv
import sqlite3 as sql

def connection() -> tuple[sql.Connection, sql.Cursor]:
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
        CREATE TABLE IF NOT EXISTS jeux (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom VARCHAR(255),
            plateforme VARCHAR(50),
            annee INTEGER,
            genre VARCHAR(50),
            editeur varchar(50),
            ventes_AN DECIMAL(5,2),
            ventes_UE DECIMAL(5,2),
            ventes_JP DECIMAL(5,2),
            ventes_autres DECIMAL(5,2),
            ventes_global DECIMAL(5,2)
        )
    """)

    # On commit et on ferme la connexion
    conn.commit()
    conn.close()



def ajout_des_valeurs(conn: sql.Connection, cursor: sql.Cursor) -> None:
    """
    Fonction qui permet d'ajouter les valeurs du fichier vgsales.csv dans la table jeux_video de la base de données vgsales.db.

    Args:
        (sql.Connection, sql.Cursor): Cette fonction prend en argument la connexion et le curseur de la base de données vgsales.db.

    Returns:
        (None): Cette fonction ne retourne rien.
    """

    #Déclaration des variables
    est_ok = False

    # Ouverture du fichier vgsales.csv
    with open('vgsales.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader)

        # Ajout des valeurs dans la table jeux_video
        for row in reader:
            # On enlève la première valeur de la liste car elle est générée automatiquement
            row = row[1:]
            est_ok = True

            # On vérifie si toutes les valeurs sont différentes de 'N/A'
            for value in row[1:]:
                if value == 'N/A':
                    est_ok = False

            # On ajoute la ligne dans la base de données si toutes les valeurs sont différentes de 'N/A'
            if est_ok:
                try:
                    cursor.execute("INSERT INTO jeux (nom, plateforme, annee, genre, editeur, ventes_AN, ventes_UE, ventes_JP, ventes_autres, ventes_global) VALUES (?,?,?,?,?,?,?,?,?,?)", row)
                except Exception as e:
                    print(e)
            else:
                print(f"La ligne {row} n'a pas été ajoutée à la base de données car une des valeurs est égale à 'N/A'.")

        # On commit et on ferme la connexion
        conn.commit()
        conn.close()





if __name__ == '__main__':
    # Connexion à la base de données
    conn, cursor = connection()

    # Création de la table jeux_video
    creation_table(conn, cursor)
    
    conn, cursor = connection()
    # Ajout des valeurs du fichier vgsales.csv dans la table jeux_video
    ajout_des_valeurs(conn, cursor)
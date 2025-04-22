import psycopg2
import pandas as pd
import requests
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# Informations de connexion
host= os.getenv("koyeb_postgres_host")
dbname = os.getenv("koyeb_postgres_db")
user = os.getenv("koyeb_postgres_user")
password = os.getenv("koyeb_postgres_password")

# Connexion à la base de données
conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)

# Création d'un curseur pour exécuter des requêtes
cursor = conn.cursor()

# Lecture d'une table avec une requête SQL
query = "SELECT * FROM tracks;" 
cursor.execute(query)

# Récupérer les résultats dans un DataFrame pandas
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()
df_tracks = pd.DataFrame(rows, columns=columns)

# Fermer la connexion
cursor.close()
conn.close()


# Préparer une liste vide pour stocker les résultats
lyrics_data = []

# URL de l'API
url = "https://musixmatch-lyrics-songs.p.rapidapi.com/songs/lyrics"


headers = {
	"x-rapidapi-key": os.getenv("rapidapi_key"),
	"x-rapidapi-host": os.getenv("rapidapi_host")
}


def escape_quotes(text):
    return text.replace("'", "").replace('"', '')


# Boucle sur chaque ligne du DataFrame
for index, row in df_tracks.iterrows():



    # Récupérer le nom de la chanson et de l'artiste
    track_name = escape_quotes(row["track_name"])
    artist_name = escape_quotes(row["artist_name"])

    # Paramètres de la requête API, sans guillemets autour des valeurs
    querystring = {"t": track_name, "a": artist_name}

    print(querystring)

    # Faire la requête GET
    response = requests.get(url, headers=headers, params=querystring)
    
    print(response.text)

    # Ajouter le résultat dans la liste
    lyrics_data.append({
        'track_name': track_name,
        'artist_name': artist_name,
        'parole': response.text
    })
    
    # Pause de 1 seconde entre chaque itération
    time.sleep(1)

# Créer un DataFrame avec les résultats
lyrics_df = pd.DataFrame(lyrics_data)

#Stockage des données dans un fichier CSV
#lyrics_df.to_csv('lyrics_morceaux.csv', index=False)


# Connexion à la base de données PostgreSQL sur Koyeb
conn = psycopg2.connect(
    host= os.getenv("koyeb_postgres_host")
    database = os.getenv("koyeb_postgres_db")
    user = os.getenv("koyeb_postgres_user")
    password = os.getenv("koyeb_postgres_password")
)

# Créer une chaîne de connexion SQLAlchemy à partir de la connexion psycopg2
# Cette chaîne de connexion sera utilisée pour interagir avec pandas et SQLAlchemy
engine = create_engine(f'postgresql+psycopg2://koyeb-adm:npg_B8ywKaiYo9jd@ep-blue-silence-a21izm04.eu-central-1.pg.koyeb.app:5432/koyebdb')

# Insérer le DataFrame dans la base de données PostgreSQL
lyrics_df.to_sql('parole_morceaux', engine, index=False, if_exists='replace')

# Fermer la connexion
conn.close()

print("Données insérées avec succès dans la base de données PostgreSQL.")

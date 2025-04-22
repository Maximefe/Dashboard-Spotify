import psycopg2
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import requests
from dotenv import load_dotenv
import os

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Connexion à l'API Spotify
client_id = os.getenv("spotify_id")
client_secret = os.getenv("spotify_secret")
redirect_uri = os.getenv("spotify_uri")


sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                               client_secret=client_secret,
                                               redirect_uri=redirect_uri,
                                               scope=["user-library-read", "user-top-read", "playlist-read-private", "user-read-recently-played", "user-read-playback-state", "user-read-currently-playing"]
                                               ))


# info sur les artiste avec lasf.fm  
api_key_lastfm = os.getenv("api_lastfm")
# URL de l'API de Last.fm
url = 'http://ws.audioscrobbler.com/2.0/'


# Connexion à la base de données PostgreSQL sur Koyeb
conn = psycopg2.connect(
    host= os.getenv("koyeb_postgres_host")
    database = os.getenv("koyeb_postgres_db")
    user = os.getenv("koyeb_postgres_user")
    password = os.getenv("koyeb_postgres_password")
)

cursor = conn.cursor()

# Créer les tables "artists" et "tracks" si elles n'existent pas

# Création de la table tracks
create_tracks_table = '''
CREATE TABLE IF NOT EXISTS tracks (
    track_id VARCHAR(255) PRIMARY KEY,
    track_name VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255),
    artist_name VARCHAR(255),
    popularity INT,
    duration_ms INT,
    explicit BOOLEAN,
    album_name VARCHAR(255),
    album_id VARCHAR(255),
    album_release_date VARCHAR(255),
    album_type VARCHAR(255),
    album_total_tracks INT,
    album_images VARCHAR(255),
    preview_url VARCHAR(255),
    external_urls VARCHAR(255)
);
'''
# Création de la table artists
create_artists_table = '''
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL,
    popularity INT,
    genres TEXT[],
    followers INT,
    external_urls VARCHAR(255)
);
'''

# Création de la table info artists
create_artists_info_table = '''
CREATE TABLE IF NOT EXISTS artists_info (
    mbid VARCHAR(255),
    artist_name VARCHAR(255),
    url VARCHAR(255),
    listeners VARCHAR(255),
    playcount VARCHAR(255),
    similar_artists VARCHAR(255),
    tags VARCHAR(255),
    bio_summary TEXT
);
'''


# Exécuter les commandes de création des tables
cursor.execute(create_tracks_table)
cursor.execute(create_artists_table)
cursor.execute(create_artists_info_table)


# Récupérer les 50 morceaux les plus écoutés
top_tracks = sp.current_user_top_tracks(limit=50, time_range='long_term')  # long_term = depuis toujours
tracks_data = []
for track in top_tracks['items']:
    tracks_data.append({
        'track_id': track['id'],
        'track_name': track['name'],
        'artist_id': track['artists'][0]['id'],
        'artist_name': track['artists'][0]['name'],
        'popularity': track['popularity'],
        'duration_ms': track['duration_ms'],
        'explicit': track['explicit'],
        'album_name': track['album']['name'],
        'album_id': track['album']['id'],
        'album_release_date': track['album']['release_date'],
        'album_type': track['album']['album_type'],
        'album_total_tracks': track['album']['total_tracks'],
        'album_images': track['album']['images'][0]['url'] if track['album']['images'] else None,
        'preview_url': track['preview_url'],
        'external_urls': track['external_urls']['spotify']
    })

tracks_df = pd.DataFrame(tracks_data)

# Insérer les morceaux
for data in tracks_data:
    cursor.execute("""
        INSERT INTO tracks (track_id, track_name, artist_id, artist_name, popularity, duration_ms, explicit, album_name, album_id, album_release_date, album_type, album_total_tracks, album_images, preview_url, external_urls)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data['track_id'],
        data['track_name'],
        data['artist_id'],
        data['artist_name'],
        data['popularity'],
        data['duration_ms'],
        data['explicit'],
        data['album_name'],
        data['album_id'],
        data['album_release_date'],
        data['album_type'],
        data['album_total_tracks'],
        data['album_images'],
        data['preview_url'],
        data['external_urls']
    ))


# Récupérer ton top des artistes
top_artists = sp.current_user_top_artists(limit=50, time_range='long_term')  # long_term = depuis toujours
artists_data = []
for artist in top_artists['items']:
    artists_data.append({
        'artist_id': artist['id'],
        'artist_name': artist['name'],
        'popularity': artist['popularity'],
        'genres': artist['genres'],  # Liste des genres
        'followers': artist['followers']['total'],  # Nombre de followers
        'external_urls': artist['external_urls']['spotify'] # URL vers l'artiste sur Spotify
    })

artists_df = pd.DataFrame(artists_data)

# Insérerles artistes 
for data in artists_data:
    cursor.execute("""
        INSERT INTO artists (artist_id, artist_name, popularity, genres, followers, external_urls)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data['artist_id'],
        data['artist_name'],
        data['popularity'],
        data['genres'],
        data['followers'],
        data['external_urls']
    ))


artiste_tracks_liste = tracks_df['artist_name'].unique()
artiste_artists_liste = artists_df['artist_name'].unique()

artists_list = list(set(artiste_artists_liste) | set(artiste_tracks_liste))

# Fonction pour récupérer les informations sur un artiste
def get_artist_info(artist_name):
    # Paramètres de la requête
    params = {
        'method': 'artist.getInfo',     # Méthode pour obtenir des informations sur l'artiste
        'artist': artist_name,          # Nom de l'artiste
        'api_key': api_key_lastfm,      # Clé API
        'format': 'json'                # Format de la réponse (JSON)
    }

    # Faire la requête GET à l'API
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        if 'artist' in data:
            artist_data = data['artist']

            # Informations de base avec gestion des clés manquantes
            artist_info = {
                'mbid': artist_data.get('mbid', 'N/A'),  # Utilise 'N/A' si 'mbid' n'existe pas
                'artist_name': artist_data['name'],
                'url': artist_data['url'],
                'listeners': artist_data['stats'].get('listeners', 'N/A'),
                'playcount': artist_data['stats'].get('playcount', 'N/A')
            }


            # Artistes similaires
            similar_artists = [(artist['name']) for artist in artist_data.get('similar', {}).get('artist', [])]
            
            # Tags
            tags = [tag['name'] for tag in artist_data.get('tags', {}).get('tag', [])]
            
            # Biographie
            bio_summary = artist_data.get('bio', {}).get('summary', 'N/A')
            
            # Organiser les informations dans un dictionnaire pour DataFrame
            artist_info['similar_artists'] = similar_artists
            artist_info['tags'] = tags
            artist_info['bio_summary'] = bio_summary
            
            return artist_info
        else:
            print(f"Aucune donnée trouvée pour l'artiste : {artist_name}")
            return None
    else:
        print(f"Erreur lors de la requête pour {artist_name}: {response.status_code}")
        return None

# Fonction pour récupérer les infos pour plusieurs artistes
def get_artists_info(artists_list):
    artist_info_list = []

    for artist in artists_list:
        artist_info = get_artist_info(artist)
        if artist_info:
            artist_info_list.append(artist_info)
    
    # Convertir la liste de dictionnaires en DataFrame
    df = pd.DataFrame(artist_info_list)
    return artist_info_list, df

# Récupérer les informations pour tous les artistes dans la liste
artist_info_list, df_artists_info = get_artists_info(artists_list)


# Insérerles infos sur les artistes 
for data in artist_info_list:
    cursor.execute("""
        INSERT INTO artists_info (mbid, artist_name, url, listeners, playcount, similar_artists, tags, bio_summary)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data['mbid'],
        data['artist_name'],
        data['url'],
        data['listeners'],
        data['playcount'],
        data['similar_artists'],
        data['tags'],
        data['bio_summary']
    ))

#df_artists_info.to_csv('artists_API_info.csv', index=False)
#tracks_df.to_csv('mes_tracks.csv', index=False)
#artists_df.to_csv('mes_artists.csv', index=False)

# Valider la transaction
conn.commit()

# Fermer la connexion
cursor.close()
conn.close()
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
import re  # Untuk pembersihan regex

class SpotifyMetadataExtractorOperator(BaseOperator):
    def __init__(self, postgres_conn_id, source_query, target_table, client_id, client_secret, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_query = source_query
        self.target_table = target_table
        self.client_id = client_id
        self.client_secret = client_secret

    import re  # Untuk pembersihan regex

    def clean_input(self, text):
        """Clean and standardize input text (e.g., song title and artist name)."""
        # Hapus karakter-karakter yang tidak diinginkan dengan regex
        cleaned_text = re.sub(r'[^\w\s]', '', text)  # Menghapus titik, koma, kutip, dll
        cleaned_text = re.sub(r'[/\\()\-_\t]', '',
                              cleaned_text)  # Menghapus backslash, garis miring, kurung, tab, dan tanda minus
        return cleaned_text.strip().lower()  # Menghapus spasi ekstra dan mengubah ke huruf kecil

    def get_all_spotify_metadata(self, song_title, artist_name):
        """Fetch metadata from Spotify for a given song title and artist name."""
        # Set up Spotify client
        auth_manager = SpotifyClientCredentials(client_id=self.client_id, client_secret=self.client_secret)
        sp = spotipy.Spotify(auth_manager=auth_manager)

        # Clean input
        song_title = self.clean_input(song_title)
        artist_name = self.clean_input(artist_name)

        # Create search query for track
        query = f"track:{song_title} artist:{artist_name}"

        # Initialize variables for pagination
        offset = 0
        limit = 50
        all_tracks = []

        while True:
            results = sp.search(q=query, type="track", limit=limit, offset=offset)
            items = results["tracks"]["items"]
            if not items:
                break

            # Collect track metadata
            for track in items:
                all_tracks.append({
                    "track_name": track["name"],
                    "artist_name": ", ".join([artist["name"] for artist in track["artists"]]),
                    "album_name": track["album"]["name"],
                    "release_date": track["album"]["release_date"],  # This is already in the correct format
                    "isrc": track["external_ids"].get("isrc", "N/A"),
                    "spotify_track_id": track["id"],
                })

            # Increment offset for pagination
            offset += limit
        self.log.info(f"example tracks: {all_tracks}")
        return all_tracks

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Step 1: Fetch songs from the source query
        records = postgres_hook.get_records(self.source_query)
        self.log.info(f"Fetched {len(records)} songs.")

        # DataFrame untuk menampung metadata yang akan dimasukkan ke DB
        all_data = []

        # Step 2: Process each record and retrieve metadata
        for record in records:
            self.log.info(f"Processing record: {record}")
            try:
                song_title = record[0]  # Song title
                artist_name = record[1]  # Artist name

                if song_title and artist_name:
                    metadata = self.get_all_spotify_metadata(song_title, artist_name)
                    for track in metadata:
                        # Pastikan release_date valid
                        release_date = pd.to_datetime(track['release_date'], errors='coerce').date() if track[
                            'release_date'] else None

                        # Bersihkan dan ubah semua kolom menjadi huruf kecil
                        row = [
                            self.clean_input(track['isrc']),
                            self.clean_input(track['spotify_track_id']),
                            self.clean_input(track['track_name']),
                            self.clean_input(track['artist_name']),
                            self.clean_input(track['album_name']),
                            release_date,
                            pd.to_datetime('now'),
                            pd.to_datetime('now')
                        ]

                        # Menambahkan row ke DataFrame
                        all_data.append(row)

                else:
                    self.log.info(f"Skipping song with missing title or artist: {record}")

            except IndexError:
                self.log.error(f"Error accessing record: {record}. Skipping this record.")
                continue

        # Setelah semua data terkumpul, konversikan ke DataFrame
        if all_data:
            df = pd.DataFrame(all_data, columns=[
                'isrc', 'spotify_track_id', 'track_name', 'artist_name', 'album_name', 'release_date', 'added_at',
                'updated_at'
            ])

            # Step 3: Hapus duplikat berdasarkan kombinasi isrc dan spotify_track_id
            df = df.drop_duplicates(subset=['isrc', 'spotify_track_id'],
                                    keep='first')  # Keep 'first' to keep the first occurrence

            # Step 4: Siapkan query upsert
            for index, row in df.iterrows():
                upsert_query = f"""
                INSERT INTO {self.target_table} (isrc, spotify_track_id, track_name, artist_name, album_name, release_date, added_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (isrc, spotify_track_id) 
                DO UPDATE 
                    SET track_name = EXCLUDED.track_name,
                        artist_name = EXCLUDED.artist_name,
                        album_name = EXCLUDED.album_name,
                        release_date = EXCLUDED.release_date,
                        updated_at = EXCLUDED.updated_at;
                """

                try:
                    # Execute upsert query for each row
                    postgres_hook.run(upsert_query, parameters=tuple(row))
                    self.log.info(f"Upserted track: {row['track_name']}")
                except Exception as e:
                    self.log.error(f"Failed to upsert track {row['track_name']}: {e}")

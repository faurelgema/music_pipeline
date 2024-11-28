from airflow import DAG
from datetime import datetime, timedelta
import json
import os
from plugins.custom_operator.google_sheet_to_postgresql import GoogleSheetToPostgresOperator
from plugins.custom_operator.spotify_crawler import SpotifyMetadataExtractorOperator
from plugins.custom_operator.youtube_crawler import YouTubeMetadataExtractorOperator
from plugins.custom_operator.mysql_to_postgres import MySqlToPostgresOperator


# Default arguments
default_args = {
    'owner': 'Gema',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 28, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Definisi DAG
dag = DAG(
    'ETL_mapping_music_warehouse',
    default_args=default_args,
    description='Migrate data from MySQL to Postgres dynamically',
    schedule_interval='@once',
    max_active_runs=1,
    catchup=False,
    tags=['active', 'music-and-media']
)
mapping_master_songs = GoogleSheetToPostgresOperator(
        task_id="mapping_master_songs",
        google_sheet_id="1OkDM1miCXh48M23n_C4AOGPl_DW1QtIXFSdHW6Grzxg",
        sheet_name="DATA",
        postgres_conn_id="postgresql_tcm",
        target_table="demo_music.m_songs",
        column_mapping={
                "CODE": "code",
                "ORIGINAL ARTIST": "original_artist",
                "SONG TITLE": "song_title"
        },
        identifier = ['code'],
        dag=dag  # Attach to the DAG
    )
# Diberi limit 2
get_music_from_spotify_api_raw = SpotifyMetadataExtractorOperator(
        task_id='get_music_from_spotify_api_raw',
        postgres_conn_id='postgresql_tcm',
        source_query='''SELECT song_title, original_artist FROM demo_music.m_songs where original_artist != 'unknown' limit 2; ''',
        target_table='demo_music.library_music_spotify',
        client_id="",
        client_secret="",
        dag=dag
    )

# Diberi limit = 1, karena quotaExceed
get_youtube_metadata_from_api_raw = YouTubeMetadataExtractorOperator(
    task_id='get_youtube_metadata_from_api',
    postgres_conn_id='postgresql_tcm',
    source_query='''SELECT song_title, original_artist FROM demo_music.m_songs where original_artist != 'unknown' limit 1; ''',
    target_table='demo_music.library_music_youtube',
    api_key="",
    dag=dag
)

load_to_media_warehouse = MySqlToPostgresOperator(
            task_id=f'load_to_media_warehouse',
            query='''SELECT 
                    ms.code, 
                    lmy.video_id,
                    lmy.channel_id,
                    lmy.video_title,
                    lmy.channel_title,
                    sp.isrc,
                    sp.spotify_track_id,
                    sp.track_name AS spotify_track_name,
                    sp.album_name AS spotify_album,
                    sp.release_date AS spotify_release_date,
                    ms.original_artist,
                    ms.song_title
                FROM demo_music.m_songs ms
                JOIN demo_music.library_music_spotify sp
                    ON ms.original_artist LIKE '%' || sp.artist_name || '%'
                    AND ms.song_title LIKE '%' || sp.track_name || '%'
                JOIN demo_music.library_music_youtube lmy
                    ON lmy.video_title LIKE '%' || sp.track_name || '%';
                ''',
            postgres_conn_target="postgresql_tcm",
            postgres_conn="postgresql_tcm",
            db_query_from='postgres',
            target_table="demo_music.media_warehouse_raw",
            identifier=["code", "video_id", "spotify_track_id"],
            email_on_failure=True,
            email_on_retry=False,
            dag=dag
        )

# Dijalankan berurutan, transfer_google_sheet_to_postgres jika sudah selesai maka akan
# Mengambil spotify dan youtube, lalu akan dimasukkan ke warehouse raw
mapping_master_songs >> [ get_music_from_spotify_api_raw, get_youtube_metadata_from_api_raw]>> load_to_media_warehouse


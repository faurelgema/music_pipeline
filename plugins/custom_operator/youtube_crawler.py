from airflow.models import BaseOperator
from googleapiclient.discovery import build
from airflow.hooks.postgres_hook import PostgresHook
import re
import pandas as pd

class YouTubeMetadataExtractorOperator(BaseOperator):
    def __init__(self, postgres_conn_id, source_query, target_table, api_key, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_query = source_query
        self.target_table = target_table
        self.api_key = api_key

        # Setup YouTube API client
        self.youtube = build("youtube", "v3", developerKey=self.api_key)

    def clean_input(self, text):
        """Clean and standardize input text (e.g., song title and artist name)."""
        cleaned_text = re.sub(r'[^\w\s]', '', text)  # Remove special characters
        cleaned_text = re.sub(r'[/\\()\-_\t]', '', cleaned_text)  # Remove unwanted symbols
        return cleaned_text.strip().lower()  # Remove extra spaces and convert to lowercase

    def get_youtube_metadata(self, query):
        """Fetch YouTube video metadata based on a search query."""
        search_response = self.youtube.search().list(
            q=query,
            part="snippet",  # The metadata part we want
            maxResults=1,    # Retrieve a maximum of 5 videos
            type="video",    # Only search for videos
        ).execute()

        results = []
        for item in search_response.get("items", []):
            results.append({
                "video_id": item["id"]["videoId"],
                "channel_id": item["snippet"]["channelId"],
                "video_title": item["snippet"]["title"],
                "channel_title": item["snippet"]["channelTitle"],
            })
        return results

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Step 1: Fetch songs from the source query
        records = postgres_hook.get_records(self.source_query)
        self.log.info(f"Fetched {len(records)} songs.")

        # DataFrame to store the YouTube metadata to be inserted into the DB
        all_data = []

        # Step 2: Process each record and retrieve YouTube metadata
        for record in records:
            self.log.info(f"Processing record: {record}")
            try:
                song_title = record[0]  # Song title
                artist_name = record[1]  # Artist name

                if song_title and artist_name:
                    query = f"{song_title} {artist_name} official"
                    metadata = self.get_youtube_metadata(query)
                    for video in metadata:
                        # Clean and standardize data
                        row = [
                            self.clean_input(video['video_id']),
                            self.clean_input(video['channel_id']),
                            self.clean_input(video['video_title']),
                            self.clean_input(video['channel_title']),
                            pd.to_datetime('now'),  # added_at
                            pd.to_datetime('now')   # updated_at
                        ]
                        # Add row to the list
                        all_data.append(row)

                else:
                    self.log.info(f"Skipping song with missing title or artist: {record}")

            except IndexError:
                self.log.error(f"Error accessing record: {record}. Skipping this record.")
                continue

        # After gathering all data, convert it into a DataFrame
        if all_data:
            df = pd.DataFrame(all_data, columns=[
                'video_id', 'channel_id', 'video_title', 'channel_title', 'added_at', 'updated_at'
            ])

            # Step 3: Remove duplicates based on video_id and channel_id
            df = df.drop_duplicates(subset=['video_id', 'channel_id'], keep='first')

            # Step 4: Insert data into database
            try:
                postgres_hook.insert_rows(
                    self.target_table,
                    df.values.tolist(),  # Convert DataFrame to list format
                    target_fields=df.columns.tolist()  # Columns for insertion
                )
                self.log.info("Successfully inserted all YouTube videos.")
            except Exception as e:
                self.log.error(f"Failed to insert YouTube videos: {e}")

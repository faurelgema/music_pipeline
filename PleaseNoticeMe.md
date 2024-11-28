Overview
========
Project Description
This project aims to build an automated data pipeline for a music publishing company. The pipeline integrates data from digital streaming platforms (DSPs) such as Spotify and YouTube, processes the metadata, and stores it in a structured data warehouse. The goal is to provide business users with actionable insights, such as the number of YouTube videos associated with each song and the count of unique ISRCs for each track on Spotify.

The pipeline ensures data quality and integrity through validation and deduplication processes. It is designed to be scalable, reliable, and optimized for high-performance querying, enabling seamless analysis by data scientists and analysts.



## Fitur
========
- [v]  Mapping the Songs Catalog:
Automates the retrieval and processing of the internal songs catalog provided via Google Sheets.
- [v]  Retrieve Spotify Metadata:
Fetches ISRC codes and other metadata (e.g., track name, artist, album, release date) related to the mapped catalog using the Spotify API.
- [v] Retrieve YouTube Metadata:
Extracts YouTube video metadata (e.g., video ID, channel ID, video title, and artist) for songs in the catalog using the YouTube API.
- [v]  Data Integration for a Unified Warehouse:
Joins data from the internal catalog, Spotify, and YouTube into a single, clean, and structured table (media_warehouse_raw) for analysis.
- [v]  Automated Data Pipeline with Scheduler:
Implements a scheduled process that:
Updates the mapping of the song catalog.
Retrieves fresh metadata from Spotify and YouTube.
Combines data into the warehouse table, ensuring consistency and reliability.
- [v] Data Quality Assurance:
Includes deduplication, validation, and error-checking mechanisms to maintain data integrity and avoid inconsistencies.
- [v] Scalable and High-Performance Design:
Optimized for querying and reporting, designed to support growing data and analytical demands.
- [v] Pipeline Monitoring and Logging:
Ensures smooth operation and provides detailed logs for debugging and tracking updates.


## How To Try?
1. Install Docker on your device.
2. Install Astronomer CLI using the provided command, and test the installation.
3. Clone this repository to your local machine.
4. Open the repository, set up the environment, and connect to it.
5. Run astro dev init in the terminal within the project directory.
6. Add "spotipy" and "google-api-python-client==2.80.0" to requirements.txt.
7. Configure airflow_settings.yaml with your database connection details.
8. Execute the DDL to create four tables in your database.
9. Add your API key and related configurations in dags/etl_music_youtube_datawarehouse.
10. Include the PostgreSQL connection ID you defined in airflow_settings.yaml.
11. Start the Airflow environment using astro dev start
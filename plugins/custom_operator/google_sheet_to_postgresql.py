import pandas as pd
import requests
from io import StringIO
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time


class GoogleSheetToPostgresOperator(BaseOperator):
    def __init__(
            self,
            google_sheet_id: str,
            sheet_name: str,
            postgres_conn_id: str,
            target_table: str,
            column_mapping: dict,  # Mapping nama kolom Google Sheet ke database
            identifier: list,  # Kolom yang digunakan untuk upsert (unique constraint)
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.google_sheet_id = google_sheet_id
        self.sheet_name = sheet_name
        self.postgres_conn_id = postgres_conn_id
        self.target_table = target_table
        self.column_mapping = column_mapping
        self.identifier = identifier

    def read_google_sheet(self) -> pd.DataFrame:
        """
        Membaca data dari Google Sheet dan melakukan transformasi kolom.
        """
        try:
            # Construct the CSV export URL for the specific sheet
            csv_url = f'https://docs.google.com/spreadsheets/d/{self.google_sheet_id}/gviz/tq?tqx=out:csv&sheet={self.sheet_name}'
            self.log.info(f"Fetching Google Sheet from URL: {csv_url}")

            # Send the request to get the CSV data
            response = requests.get(csv_url)
            response.raise_for_status()

            # Load the CSV data into a pandas DataFrame
            df = pd.read_csv(StringIO(response.text))

            # Transformasi nama kolom sesuai dengan mapping
            df.rename(columns=self.column_mapping, inplace=True)
            self.log.info(f"Transformed columns: {df.columns.tolist()}")

            # Replace empty strings or NaN values with None
            df = df.applymap(lambda x: 'unknown' if pd.isna(x) or x == "" else x)


            # Convert all string data to lowercase
            df = df.applymap(lambda x: str(x).lower() if isinstance(x, str) else x)

            return df

        except Exception as e:
            self.log.error(f"Error while reading Google Sheet: {str(e)}")
            return pd.DataFrame()  # Return an empty DataFrame to avoid further errors

    def generate_upsert_query(self, target_fields, identifier):
        """
        Membuat query SQL untuk operasi upsert.
        """
        columns = ', '.join(target_fields)
        placeholders = ', '.join(['%s'] * len(target_fields))
        conflict_target = ', '.join(identifier)
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in target_fields if col not in identifier])

        query = f"""
            INSERT INTO {self.target_table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_set};
        """
        self.log.info(f"Generated UPSERT query: {query}")
        return query

    def execute(self, context):
        """
        Eksekusi operator untuk membaca Google Sheet dan melakukan upsert ke PostgreSQL.
        """
        start_time = time.time()

        # Membaca data dari Google Sheet
        df = self.read_google_sheet()
        if df.empty:
            self.log.info("No data found in Google Sheet. Exiting.")
            return

        # Membuat koneksi ke PostgreSQL
        postgres_hook = PostgresHook(self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Menyisipkan atau memperbarui data ke PostgreSQL
            rows = df.to_records(index=False).tolist()
            target_fields = df.columns.tolist()
            query = self.generate_upsert_query(target_fields, self.identifier)

            if rows:
                self.log.info(f"Executing upsert query: {query}")
                cursor.executemany(query, rows)
                conn.commit()
                self.log.info(f"Upserted {len(rows)} rows into {self.target_table}.")
            else:
                self.log.info("No data to upsert.")

        except Exception as e:
            self.log.error(f"Error during database operation: {e}")
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()

        execution_time = time.time() - start_time
        self.log.info(f"Execution completed in {execution_time:.2f} seconds.")

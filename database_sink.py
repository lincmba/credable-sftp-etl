import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class PostgresStorage:
    def __init__(self):
        """
        Initialize the PostgreSQL connection using environment variables.
        """
        self.dbname = os.getenv("PG_DB")
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASS")
        self.host = os.getenv("PG_HOST", "localhost")
        self.port = os.getenv("PG_PORT", "5432")
        self.table_name = os.getenv("PG_TABLE",
                                    "data_store")  # Table to store data
        self.primary_key = os.getenv("DATA_PRIMARY_ID")
        # Create SQLAlchemy engine for easy Pandas DataFrame inserts
        self.engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")

    def store_data(self, df):
        """
        Store the cleaned DataFrame into PostgreSQL.
        - Creates the table if it doesn't exist.
        - Handles schema changes dynamically.
        """
        if df is None or df.empty:
            print("No data to store.")
            return

        try:
            # Add the data to the table, creating it if necessary
            df.to_sql(self.table_name, self.engine, if_exists="replace",
                      index=False)
            print(f"Stored {len(df)} rows into {self.table_name} table.")

            if self.primary_key:
                with self.engine.connect() as con:
                    con.execute(
                        f'ALTER TABLE {self.table_name} ADD PRIMARY KEY ({self.primary_key});')

        except Exception as e:
            print(f"Error inserting data into PostgreSQL: {e}")

    def close(self):
        """Close the database connection (optional)."""
        self.engine.dispose()
        print("Database connection closed.")

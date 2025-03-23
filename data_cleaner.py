import os
import pandas as pd
import json

class DataCleaner:
    def __init__(self, directory):
        """
        Initialize the DataCleaner with a directory containing CSV and JSON files.
        """
        self.directory = directory

    def read_file(self, file_path):
        """
        Read a file based on its extension (.csv or .json) and return a Pandas DataFrame.
        """
        try:
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
            elif file_path.endswith(".json"):
                df = self.flatten_json(pd.read_json(file_path, lines=True))  # Handle nested JSONs
            else:
                raise ValueError("Unsupported file format!")
            return df
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return None

    def flatten_json(self, df):
        """
        Flatten nested JSON data by expanding nested dictionaries into separate columns.
        """
        def flatten_dict(d, parent_key='', sep='_'):
            """
            Recursively flatten a nested dictionary.
            """
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)

        # Normalize all rows in the DataFrame
        df = pd.json_normalize(df.to_dict(orient='records'), sep='_')

        return df

    def clean_column_names(self, df):
        """
        Standardize column names by:
        - Converting to lowercase
        - Replacing spaces and special characters with underscores
        - Merging multiple underscores into one
        """
        df.columns = (df.columns
                      .str.strip()
                      .str.lower()
                      .str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)  # Replace special chars with _
                      .str.replace(r'__+', '_', regex=True))  # Merge multiple underscores
        return df

    def handle_missing_values(self, df, strategy="fill", fill_value="Unknown"):
        """
        Handle missing values using different strategies:
        - "drop": Remove rows with missing values.
        - "fill": Fill missing values with a given value.
        - "interpolate": Use linear interpolation.
        """
        if strategy == "drop":
            df = df.dropna()
        elif strategy == "fill":
            df = df.fillna(fill_value)
        elif strategy == "interpolate":
            df = df.interpolate()
        return df

    def remove_duplicates(self, df):
        """
        Remove duplicate rows.
        """
        return df.drop_duplicates()

    def fix_data_types(self, df):
        """
        Convert columns to appropriate data types.
        """
        for col in df.columns:
            if df[col].dtype == 'object':  # Check if column is a string
                try:
                    df[col] = pd.to_datetime(df[col])  # Try converting to datetime
                except:
                    try:
                        df[col] = pd.to_numeric(df[col])  # Try converting to numeric
                    except:
                        pass  # Leave as string if conversion fails
        return df

    def clean_data(self, df):
        """
        Perform all cleaning steps on the DataFrame.
        """
        df = self.clean_column_names(df)
        df = self.handle_missing_values(df, strategy="fill", fill_value="Unknown")  # Default: fill missing
        df = self.remove_duplicates()
        df = self.fix_data_types(df)
        return df

    def process_all_files(self):
        """
        Process all CSV and JSON files in the directory.
        """
        for file in os.listdir(self.directory):
            if file.endswith(".csv") or file.endswith(".json"):
                file_path = os.path.join(self.directory, file)
                print(f"Processing: {file_path}")

                df = self.read_file(file_path)
                if df is not None:
                    df = self.clean_data(df)
                    print(f"Cleaned {file_path} - {len(df)} rows remaining.\n")

        print("All files processed successfully.")


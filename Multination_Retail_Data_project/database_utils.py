import yaml
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, file_path='db_creds.yaml'):
        """
        Constructor for the DatabaseConnector class.

        Args:
            file_path (str): Path to the YAML file containing database credentials.
        """
        self.file_path = file_path
        self.engine = self.init_db_engine()
    def read_db_creds(self):
        """
        Read database credentials from a YAML file.

        Returns:
            dict: A dictionary containing database connection information.
        """
        try:
            with open(self.file_path, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError:
            print(f"Error: The file '{self.file_path}' was not found.")
            return None

    def init_db_engine(self):
        """
        Initialize a SQLAlchemy engine for database connection.

        Returns:
            sqlalchemy.engine.base.Connection: A SQLAlchemy database engine.
        """
        connection_info = self.read_db_creds()
        try:
            host = connection_info.get('RDS_HOST', '')
            port = connection_info.get('RDS_PORT', '')
            database = connection_info.get('RDS_DATABASE', '')
            user = connection_info.get('RDS_USER', '')
            password = connection_info.get('RDS_PASSWORD', '')

            # Creating a SQLAlchemy engine with the provided database credentials
            engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}", isolation_level='AUTOCOMMIT')
            return engine
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

    def upload_to_db(self, table, table_name):
        """
        Upload a DataFrame to the database table.

        Args:
            table (pd.DataFrame): DataFrame to be uploaded.
            table_name (str): Name of the target database table.
        """
        with self.engine.connect() as conn:
            try:
                # Using SQLAlchemy's to_sql method to upload the DataFrame to the specified table
                table.to_sql(table_name, conn, if_exists='replace')
                print("Uploaded!")
            except Exception as e:
                print(f"Error uploading data to the database: {e}")

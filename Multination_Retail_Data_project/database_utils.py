import yaml
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    # Constructor method
    def __init__(self, file_path='db_creds.yaml'):
        # YAML file containing the relevant information to connect to the database
        self.file_path = file_path
        self.engine = self.init_db_engine()

    # Loads information from YAML file and returns it as a dictionary
    def read_db_creds(self):
        try:
            with open(self.file_path, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError:
            print(f"Error: The file '{self.file_path}' was not found.")
            return None

    # Connects to PostgreSQL using information from YAML file and returns the connection as an engine
    def init_db_engine(self):
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

    # Uploads a DataFrame to the database table
    def upload_to_db(self, table, table_name):
        with self.engine.connect() as conn:
            # Using SQLAlchemy's to_sql method to upload the DataFrame to the specified table
            table.to_sql(table_name, conn, if_exists='replace')
            print("Uploaded!")
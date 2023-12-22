import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    #Contructor method
    def __init__(self):
        #YAML file containing the relevant information in order to connect to database
        self.file_path = 'db_creds.yaml'
    #Loads information from YAML file and loads information as a dictionary
    def read_db_creds(self):
        try:
            with open(self.file_path, 'r') as file:
                data = yaml.safe_load(file)
            return data
        except FileNotFoundError:
            print(f"Error: The file '{self.file_path}' was not found.")
            return None
    #Connects to the postgresql+psycopg2 using information given in YAML file and returns said connection as engine
    def init_db_engine(self):
        connection_info = self.read_db_creds()
        try:
            host = connection_info.get('RDS_HOST', '')
            port = connection_info.get('RDS_PORT', '')
            database = connection_info.get('RDS_DATABASE', '')
            user = connection_info.get('RDS_USER', '')
            password = connection_info.get('RDS_PASSWORD', '')

            engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
            return engine.execution_options(isolation_level='AUTOCOMMIT')
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

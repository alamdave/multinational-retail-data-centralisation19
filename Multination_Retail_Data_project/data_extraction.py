import database_utils as du
import data_cleaning as dc
from sqlalchemy import inspect
import pandas as pd

class DataExtractor:
    def __init__(self, engine, table_name='legacy_users'):
        # Instantiate a database connector class from database_utils
        self.table_name = table_name
        self.engine = engine

    # Loads information from YAML file and returns it as a dictionary
    def list_db_tables(self):
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            table_names = inspector.get_table_names()
            return table_names

    def read_rds_table(self):
        with self.engine.connect() as conn:
            df = pd.read_sql_table(self.table_name, conn)
            return df

# Instantiate a database connector
database_connection = du.DatabaseConnector()

# Load data from the database table 'legacy_users'
load_db = DataExtractor(engine=database_connection.init_db_engine()).read_rds_table()

# Instantiate a data cleaner
data_cleaner = dc.DataCleaning(dataframe=load_db)

# Clean the user data
clean_data = data_cleaner.clean_user_data()

# Connect to sales data database
connect_to_sales_data = du.DatabaseConnector(file_path='sales_data_creds.yaml')

# Upload cleaned data to the database table 'dim_users'
connect_to_sales_data.upload_to_db(table=clean_data, table_name='dim_users')

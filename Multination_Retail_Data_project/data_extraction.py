import database_utils as du
from sqlalchemy import inspect
import pandas as pd
class DataExtractor :
    def __init__(self):
        #instantiate a databaseconnector class from database_utils
        self.database_con = du.DatabaseConnector()
    #Loads information from YAML file and loads information as a dictionary
    def list_db_tables(self):
        engine = self.database_con.init_db_engine()
        with engine.connect() as conn:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            return table_names
    
    def read_rds_table(self, table_name ):
        engine = self.database_con.init_db_engine()

        with engine.connect() as conn:
            df = pd.read_sql_table(table_name, conn)
            return df

#instantiate a data_extractor class
data_extractor = DataExtractor()
#selct the [1] table name from list of table names
user_table = data_extractor.list_db_tables()[1]
print(user_table)
#sleect the user table with the name user_table as a panda table
user_data_panda = data_extractor.read_rds_table(user_table)
print(user_data_panda)
# Read the table into a pandas DataFrame
#user_data_df = data_extractor.read_rds_table(dbc, user_data_table)



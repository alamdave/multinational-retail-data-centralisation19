import database_utils as du
from sqlalchemy import inspect
import pandas as pd
class DataExtractor :
    def list_db_tables(self, database_con):
        engine = database_con.init_db_engine()
        with engine.connect() as conn:
            inspector = inspect(engine)
            table_names = inspector.get_table_names()
            return table_names
    
    def read_rds_table(self, database_con, table_name ):
        engine = database_con.init_db_engine()

        with engine.connect() as conn:
            df = pd.read_sql_table(table_name, conn)
            return df


#instantiate a data_extractor class
data_extractor = DataExtractor()
#instantiate a databaseconnector class from database_utils
dbc = du.DatabaseConnector()

user_table = data_extractor.list_db_tables(dbc)[1]

print(user_table)

user_data_panda = data_extractor.read_rds_table(dbc, user_table)
print(user_data_panda)
# Read the table into a pandas DataFrame
#user_data_df = data_extractor.read_rds_table(dbc, user_data_table)



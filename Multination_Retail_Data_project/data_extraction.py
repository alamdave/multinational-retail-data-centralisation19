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

        return True


#instantiate a data_extractor class
data_extractor = DataExtractor()
#instantiate a databaseconnector class from database_utils
dbc = du.DatabaseConnector()

print(data_extractor.list_db_tables(dbc))


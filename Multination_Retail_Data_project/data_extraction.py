import database_utils as du
from sqlalchemy import inspect
class DataExtractor :
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    def read_rds_table():
        return True


#instantiate a data_extractor class
data_extractor = DataExtractor()
#instantiate a databaseconnector class from database_utils
dbc = du.DatabaseConnector()
#create a engine
engine = dbc.init_db_engine()

with engine.connect() as conn:
    #list tables
    print(data_extractor.list_db_tables(conn))

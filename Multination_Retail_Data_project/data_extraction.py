import database_utils as du
from sqlalchemy import inspect
class DataExtractor :
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    def read_rds_table():
        return True



data_extractor = DataExtractor()
dbc = du.DatabaseConnector()

engine = dbc.init_db_engine()

names = data_extractor.list_db_tables()
print(names)

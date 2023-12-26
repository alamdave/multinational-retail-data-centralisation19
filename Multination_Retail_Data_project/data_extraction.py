from sqlalchemy import inspect
import pandas as pd
import tabula
import requests

class DataExtractor:
    def __init__(self, engine=None, table_name='legacy_users'):
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
        
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages='all', stream=True)
        combined = pd.concat(dfs, ignore_index=True)
        return combined
    
    def list_number_of_stores(self, url, headers):
        response = requests.get(url=url,headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            print("NOPE")

    def retrieve_store_data(self,url,store_number):
        x = (f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')
        response = requests.get
        pass


new = DataExtractor()
url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

x = new.list_number_of_stores(url=url, headers=headers)
print(x)



from sqlalchemy import inspect
import pandas as pd
import tabula
import requests

class DataExtractor:
    def __init__(self, engine=None, table_name=None):
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
    
    def list_number_of_stores(self, number_of_stores_endpoint,headers):
        try:
            # Make a GET request to the API to retrieve the number of stores
            response = requests.get(url=number_of_stores_endpoint, headers=headers)
            
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                data = response.json()
                number_of_stores = data.get('number_stores')
                if number_of_stores is not None:
                    return number_of_stores
                else:
                    print("Error: 'number_of_stores' key not found in the response.")
                    return None
            else:
                print(f"Error: Unable to retrieve number of stores. Status Code: {response.status_code}")
                return None
        
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None

    def retrieve_stores_data(self, store_details_endpoint, number_of_stores, headers):
        try:
            # Create an empty DataFrame to store store details
            all_stores_data = pd.DataFrame()

            # Loop through store numbers and retrieve store details
            for store_number in range(0, number_of_stores):

                #
                store_url = f'{store_details_endpoint}{store_number}'
                response = requests.get(url=store_url, headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    print("Loading store: "+str(store_number),end="\r",flush=True)
                    store_data = response.json()
                    current_store = pd.DataFrame([store_data])
                    all_stores_data = pd.concat([all_stores_data, current_store], ignore_index=True)
                else:
                    print(f"Error retrieving store {store_number}. Status Code: {response.status_code}")
            print("Data loaded!")

            # Display the DataFrame with all store details
            print("\nAll Stores Data:")
            print(all_stores_data)

            return all_stores_data
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
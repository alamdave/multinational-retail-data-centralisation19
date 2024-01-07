from sqlalchemy import inspect
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    def __init__(self, engine=None):
        """
        Constructor for the DataExtractor class.

        Args:
            engine: SQLAlchemy database engine (not implemented yet).
        """
        self.engine = engine

    def list_db_tables(self):
        """
        List the tables in the connected database.

        Returns:
            list: A list of table names.
        """
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            table_names = inspector.get_table_names()
            return table_names

    def read_rds_table(self, table_name):
        """
        Read data from an RDS table.

        Args:
            table_name (str): Name of the table to read.

        Returns:
            pd.DataFrame: DataFrame containing the table data.
        """
        with self.engine.connect() as conn:
            df = pd.read_sql_table(table_name, conn)
            return df
        
    def retrieve_pdf_data(self, link):
        """
        Retrieve data from a PDF document.

        Args:
            link (str): URL or file path to the PDF document.

        Returns:
            pd.DataFrame: Combined DataFrame containing data from all pages.
        """
        dfs = tabula.read_pdf(link, pages='all', stream=True)
        combined = pd.concat(dfs, ignore_index=True)
        return combined
    
    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        """
        List the number of stores from an API.

        Args:
            number_of_stores_endpoint (str): API endpoint to retrieve store count.
            headers (dict): Headers for the API request.

        Returns:
            int: Number of stores.
        """
        try:
            response = requests.get(url=number_of_stores_endpoint, headers=headers)
            
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
        """
        Retrieve store details from an API.

        Args:
            store_details_endpoint (str): API endpoint to retrieve store details.
            number_of_stores (int): Number of stores to retrieve.
            headers (dict): Headers for the API request.

        Returns:
            pd.DataFrame: DataFrame containing store details.
        """
        try:
            all_stores_data = pd.DataFrame()

            for store_number in range(0, number_of_stores):
                store_url = f'{store_details_endpoint}{store_number}'
                response = requests.get(url=store_url, headers=headers)

                if response.status_code == 200:
                    print("Loading store: " + str(store_number), end="\r", flush=True)
                    store_data = response.json()
                    current_store = pd.DataFrame([store_data])
                    all_stores_data = pd.concat([all_stores_data, current_store], ignore_index=True)
                else:
                    print(f"Error retrieving store {store_number}. Status Code: {response.status_code}")
            print("Data loaded!")

            print("\nAll Stores Data:")
            print(all_stores_data)

            return all_stores_data
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None

    def extract_from_s3(self, address):
        """
        Extract data from an S3 bucket.

        Args:
            address (str): S3 object address.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        address_list = address.split("/")
        client = boto3.client("s3")
        client.download_file(address_list[2], address_list[3], "local.csv")
        df = pd.read_csv(filepath_or_buffer="local.csv")
        return df

    def extract_date_events(self, address):
        """
        Extract data from a JSON file using a URL.

        Args:
            address (str): URL of the JSON file.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        response = requests.get(address)
        if response.status_code == 200:
            data = response.json()
            db = pd.DataFrame(data)
            return db
        else:
            print(f"Failed to fetch JSON file. Status code: {response.status_code}")

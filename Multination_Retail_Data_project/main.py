import data_extraction as de
import database_utils as du
import data_cleaning as dc

class RunData:
    def __init__(self):
        self.user_db_connector = du.DatabaseConnector(file_path="db_creds.yaml")
        self.link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        self.sales_db_connector = du.DatabaseConnector(file_path="sales_data_creds.yaml")
        self.database_cleaner = dc.DataCleaning()
        self.database_extractor = de.DataExtractor(engine=self.user_db_connector.init_db_engine())

    def clean_user(self):
        print("Starting user extraction and cleaning...\n")
        legacy_users = self.database_extractor.list_db_tables()[1]
        db = self.database_extractor.read_rds_table(table_name=legacy_users)
        print("Data extracted! Cleaning data...\n")
        self.database_cleaner.set_data_frame(db)
        cleaned_user_data = self.database_cleaner.clean_user_data()
        self.upload_db(table=cleaned_user_data, table_name="dim_users")
        print("Success! Cleaned and uploaded as 'dim_users'")

    def clean_card(self):
        print("Starting card information extraction and cleaning...\n")
        db = self.database_extractor.retrieve_pdf_data(link=self.link)
        print("Data extracted! Cleaning data...\n")
        self.database_cleaner.set_data_frame(db)
        cleaned_card_data = self.database_cleaner.clean_card_data()
        self.upload_db(table=cleaned_card_data, table_name="dim_card_details")
        print("Success! Cleaned and uploaded as 'dim_card_details'")

    def clean_stores(self):
        print("Starting stores extraction and cleaning...\n")
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        store_details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

        print("Retrieving number of stores...\n")
        number_of_stores = self.database_extractor.list_number_of_stores(number_of_stores_endpoint=number_of_stores_endpoint,headers=headers)
        print(f"Total number of stores:{number_of_stores} \n")

        print("Extracting data...\n")
        all_stores_data = self.database_extractor.retrieve_stores_data(store_details_endpoint=store_details_endpoint,headers=headers,number_of_stores=number_of_stores)
        print(f"All store data collected!\n")

        self.database_cleaner.set_data_frame(all_stores_data)
        db = self.database_cleaner.clean_store_data()

        self.sales_db_connector.upload_to_db(table=db, table_name="dim_store_details")
        print("Success! Cleaned and uploaded as 'dim_store_details'")

    def clean_product(self):
        print("Starting product extraction and cleaning...\n")
        raw_product_data = self.database_extractor.extract_from_s3("s3://data-handling-public/products.csv")
        self.database_cleaner.set_data_frame(raw_product_data)
        standarize_weights = self.database_cleaner.convert_product_weights()
        self.database_cleaner.set_data_frame(standarize_weights)
        db = self.database_cleaner.clean_products_data()

        self.sales_db_connector.upload_to_db(table=db, table_name="dim_products")
        print("Success! Cleaned and uploaded as 'dim_product_details'")

    def clean_orders(self):
        print("Starting orders extraction and cleaning...\n")
        order_table = self.database_extractor.list_db_tables()[2]
        db = self.database_extractor.read_rds_table(table_name=order_table)

        self.database_cleaner.set_data_frame(db)
        db = self.database_cleaner.clean_orders_data()

        self.sales_db_connector.upload_to_db(table=db, table_name="orders_table")
        print("Success! Cleaned and uploaded as 'orders_table'")
    
    def clean_date_events(self):
        print("Starting date extraction and cleaning...\n")
        address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        raw_date_events = self.database_extractor.extract_date_events(address=address)

        self.database_cleaner.set_data_frame(raw_date_events)
        db = self.database_cleaner.clean_date_events_data()

        self.sales_db_connector.upload_to_db(table=db, table_name="dim_date_times")
        print("Success! Cleaned and uploaded as 'dim_date_times'")

    def upload_db(self, table, table_name):
        self.sales_db_connector.upload_to_db(table=table,table_name=table_name)
        
if __name__ == "__main__":
    run_data_instance = RunData()

    run_data_instance.clean_user()
    run_data_instance.clean_card()
    run_data_instance.clean_stores()
    run_data_instance.clean_product()
    run_data_instance.clean_orders()
    run_data_instance.clean_date_events()
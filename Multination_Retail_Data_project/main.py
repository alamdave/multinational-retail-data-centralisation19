import data_extraction as de
import database_utils as du
import data_cleaning as dc

class RunData:
    def __init__(self):


        #user details database connection for retrieval instance
        self.user_db_connector = du.DatabaseConnector(file_path="db_creds.yaml")
        #PDF file for card details retrieval
        self.link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

        #sales database connction for uploads instance
        self.sales_db_connector = du.DatabaseConnector(file_path="sales_data_creds.yaml")
        #initiante instance of a cleaner
        self.database_cleaner = dc.DataCleaning()

        #Database extractor instance
        self.database_extractor = de.DataExtractor(engine=self.user_db_connector.init_db_engine(), table_name='legacy_users')


    def clean_user(self):
        print("Starting user extraction and cleaning...\n")
        db = self.database_extractor.read_rds_table()
        print(db)
        #set the desired database to be cleaned and return cleaned table
        print("Data extracted! Cleaning data...\n")
        self.database_cleaner.set_data_frame(db)
        cleaned_user_data = self.database_cleaner.clean_user_data()
        print(cleaned_user_data)
        #upload cleaned user data table
        self.upload_db(table=cleaned_user_data,table_name="dim_users")
        print("Success! Cleaned and uploaded as 'dim_users'")


    def clean_card(self):
        print("Starting card information extraction and cleaning...\n")
        #Extract raw data from pdf link and load in db
        db = self.database_extractor.retrieve_pdf_data(link=self.link)

        print("Data extracted! Cleaning data...\n")
        #loads raw card details into database_cleaner
        self.database_cleaner.set_data_frame(db)
        cleaned_card_data = self.database_cleaner.clean_card_data()

        #Uploads database with name "dim_card_details"
        print(cleaned_card_data)
        self.upload_db(table=cleaned_card_data,table_name="dim_card_details")
        print("Success! Cleaned and uploaded as 'dim_card_details'")

    def clean_stores(self):
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        store_details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

        #Connect to the sales_data database, using the credentials in sales_data_creds.yaml file

        #Retrieves the number of stores
        print("Retrieving number of stores...\n")
        number_of_stores = self.database_extractor.list_number_of_stores(number_of_stores_endpoint=number_of_stores_endpoint,headers=headers)

        print(f"Total number of stores:{number_of_stores} \n")

        #Collects all store data
        print("Extracting data...\n")
        all_stores_data = self.database_extractor.retrieve_stores_data(store_details_endpoint=store_details_endpoint,headers=headers,number_of_stores=number_of_stores)
        print(f"All store data collected!\n")

        #clean data...
        self.database_cleaner.set_data_frame(all_stores_data)
        db = self.database_cleaner.clean_store_data()

        self.sales_db_connector.upload_to_db(table=db, table_name="dim_store_details")

    
    def upload_db(self, table, table_name):
        self.sales_db_connector.upload_to_db(table=table,table_name = table_name)


if __name__ == "__main__":
    run_data_instance = RunData()
    #run_data_instance.clean_user()
    #run_data_instance.clean_card()
    #run_data_instance.clean_stores()

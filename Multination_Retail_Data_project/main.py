import data_extraction as de
import database_utils as du
import data_cleaning as dc

class RunData:
    def __init__(self, file_path=None, table=None):
        self.database_connector = du.DatabaseConnector(file_path=file_path)
        self.database_extractor = de.DataExtractor(engine=self.database_connector.init_db_engine(), table_name=table)
        self.data_cleaner = dc.DataCleaning(self.database_extractor.read_rds_table())

    def clean_card(self):
        return self.data_cleaner.clean_card_data()
    def upload_db(self, table, table_name):
        self.database_connector.upload_to_db(table=table,table_name = table_name)


if __name__ == "__main__":
    sales_data = 'sales_data_creds.yaml'
    raw_card_table = 'raw_card_data'

    run_data_instance = RunData(file_path=sales_data, table=raw_card_table)
    cleaned_card_data = run_data_instance.clean_card()

    run_data_instance.upload_db(cleaned_card_data, 'dim_card_details')

    """ x = DataExtractor().retrieve_pdf_data(link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    # Connect to sales data database
    connect_to_sales_data = du.DatabaseConnector(file_path='sales_data_creds.yaml')

    # Upload cleaned data to the database table 'dim_users'
    connect_to_sales_data.upload_to_db(table=x, table_name='raw_card_data') """

    """ # Instantiate a database connector
    database_connection = du.DatabaseConnector()

    # Load data from the database table 'legacy_users'
    load_db = DataExtractor(engine=database_connection.init_db_engine()).read_rds_table()

    # Instantiate a data cleaner
    data_cleaner = dc.DataCleaning(dataframe=load_db)

    # Clean the user data
    clean_data = data_cleaner.clean_user_data()

    # Connect to sales data database
    connect_to_sales_data = du.DatabaseConnector(file_path='sales_data_creds.yaml')

    # Upload cleaned data to the database table 'dim_users'
    connect_to_sales_data.upload_to_db(table=clean_data, table_name='dim_users')
    """
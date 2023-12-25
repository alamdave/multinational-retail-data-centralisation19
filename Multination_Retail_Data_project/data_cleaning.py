import pandas as pd
import numpy as np
import data_extraction as de
from dateutil.parser import parse

class DataCleaning:
    def __init__(self, dataframe):
        self.df = dataframe

    def clean_user_data(self):
        # Display original DataFrame
        print("Original DataFrame:")
        print(self.df)

        # Drop duplicate rows
        self.df['first_name'] = self.df['first_name'].astype(str)
        self.df['last_name'] = self.df['last_name'].astype(str)

        # Handle errors with dates
        try:
            # Convert to datetime format and handle incorrect date formats with NaT
            self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], errors='coerce')
            self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce')
        except ValueError as e:
            print(f"Error converting date columns: {e}")
            self.df['date_of_birth'] = pd.NaT
            self.df['join_date'] = pd.NaT

        self.df['company'] = self.df['company'].astype(str)
        self.df['email_address'] = self.df['email_address'].astype(str)
        self.df['address'] = self.df['address'].astype(str)
        self.df['country'] = self.df['country'].astype(str)

        # Create a dictionary to map country names to country codes
        country_code_dict = {'United States': 'US', 'United Kingdom': 'GB', 'Germany': 'GE'}
        codes = ['US', 'GB', 'GE']
        #use dictionary country_code_dict to set country codes
        self.df['country_code'] = self.df['country'].replace(country_code_dict)
        
        # Turn the country code column into a category data type with @codes as the categories
        self.df['country_code'] = pd.Categorical(self.df['country_code'], categories=codes)

        # Convert 'phone_number' to string, using regex replace (,),-, ,. with empty space
        self.df['phone_number'] = self.df['phone_number'].replace({r'\(': '', r'\)': '', r'-': '', r' ': '', r'\.': ''}, regex=True)

        # Set datatype for unique id, only values with a lenght of are 36 are valid otherwise Nan
        self.df['user_uuid'] = self.df['user_uuid'].astype(str)
        self.df.loc[self.df['user_uuid'].str.len() != 36, 'user_uuid'] = np.nan

        # Drop rows with missing values
        self.df = self.df.dropna()

        # Reset index and set datatype of 'index' to int32
        self.df = self.df.reset_index(drop=True)
        self.df['index'] = self.df.index.astype('int32')

        # Display cleaned DataFrame
        print("\nCleaned DataFrame:")
        print(self.df)

# Instantiate the DataCleaning and DataExtractor classes
data_extract = de.DataExtractor()

# Read the user table into a DataFrame
user_table = data_extract.read_rds_table()

# Clean the table user_table
cleandata = DataCleaning(user_table)
cleandata.clean_user_data()
import pandas as pd
import numpy as np
from dateutil.parser import parse
from datetime import datetime

class DataCleaning:
    def __init__(self, dataframe=None):
        self.df = dataframe

    def clean_user_data(self ):
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
        # Use dictionary country_code_dict to set country codes
        self.df['country_code'] = self.df['country'].replace(country_code_dict)

        # Turn the country code column into a category data type with @codes as the categories
        self.df['country_code'] = pd.Categorical(self.df['country_code'], categories=codes)

        # Convert 'phone_number' to string, using regex replace (,),-, ,. with empty space
        self.df['phone_number'] = self.df['phone_number'].replace({r'\(': '', r'\)': '', r'-': '', r' ': '', r'\.': ''}, regex=True)

        # Set datatype for unique id, only values with a length of 36 are valid, otherwise NaN
        self.df['user_uuid'] = self.df['user_uuid'].astype(str)
        self.df.loc[self.df['user_uuid'].str.len() != 36, 'user_uuid'] = np.nan

        # Drop rows with missing values
        self.df = self.df.dropna()

        # Reset index and set datatype of 'index' to int32
        self.df = self.df.reset_index(drop=True)
        self.df['index'] = self.df.index.astype('int32')

        # Display cleaned DataFrame
        print("\nCleaned DataFrame!")
        return self.df

    def clean_card_data(self):
        # Remove duplicate and null columns
        self.df = self.df.drop(self.df.columns[[5, 6]], axis=1)

        # Create a Dictionary of card lengths for card providers
        digit_dictionary = {'16 digit': 16, '15 digit': 15, '13 digit': 13, '19 digit': 19, 'Diners Club / Carte Blanche': 14, 'Discover': 16, 'American Express': 15, 'Maestro': 12}

        # Map card lengths to 'card_length' column
        self.df['card_length'] = self.df['card_provider'].apply(lambda x: next((length for substring, length in digit_dictionary.items() if substring in x), None))
        self.df['card_length'] = self.df['card_length'].astype('Int64')

        # Remove specified strings and preprocess 'card_provider'
        remove_strings = [' 16 digit', ' 15 digit', ' 13 digit', ' 19 digit']
        self.df['card_provider'] = self.df['card_provider'].str.replace('|'.join(remove_strings), '', regex=True)
        self.df['card_provider'] = self.df['card_provider'].str.upper().astype(str)

        # Set invalid 'card_number' values to NaN
        self.df.loc[self.df['card_number'].str.len() != self.df['card_length'], 'card_number'] = np.nan
        self.df['card_number'] = self.df['card_number'].astype(str)

        # Convert 'expiry_date' to datetime format with error handling
        try:
            self.df['expiry_date'] = pd.to_datetime(self.df['expiry_date'], format='%m/%y', errors='coerce')
        except ValueError as e:
            print(f"Error converting 'expiry_date': {e}")

        # Filter by current date and drop NaN values
        current_date = datetime.now()
        self.df = self.df[self.df['expiry_date'] >= current_date].dropna()

        # Reset index and set datatype of 'index' to int32
        self.df = self.df.reset_index(drop=True)
        self.df['index'] = self.df.index.astype('int32')

        print("\nCleaned Card Data!")
        return self.df
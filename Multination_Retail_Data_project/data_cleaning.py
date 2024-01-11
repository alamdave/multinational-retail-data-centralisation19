import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.parser import parse
import re


class DataCleaning:
    def __init__(self, dataframe=None):
        self.df = dataframe

    def set_data_frame(self, dataframe):
        """Set the DataFrame for cleaning."""
        self.df = dataframe

    def date_parsing(self, date):
        """Parse dates in various formats to datetime objects."""
        try:
            return pd.to_datetime(date)
        except:
            try:
                return pd.to_datetime(date, format='%B %Y %d')
            except:
                return pd.NaT

    def clean_user_data(self):
        """Clean user data in the DataFrame."""
        # Convert first and last names to string
        
        self.df["first_name"] = self.df["first_name"].astype(str)
        self.df["last_name"] = self.df["last_name"].astype(str)

        # Handle date conversion errors
        self.df["date_of_birth"] = self.df["date_of_birth"].apply(self.date_parsing)
        self.df["join_date"] = self.df["join_date"].apply(self.date_parsing)


        # Convert other columns to string
        self.df["company"] = self.df["company"].astype(str)
        self.df["email_address"] = self.df["email_address"].astype(str)
        self.df["address"] = self.df["address"].astype(str)
        self.df["country"] = self.df["country"].astype(str)

        # Map country names to country codes
        country_code_dict = {'United States': 'US', 'United Kingdom': 'GB', 'Germany': 'GE'}
        self.df["country_code"] = self.df["country"].replace(country_code_dict)

        # Convert country code to categorical type
        self.df["country"] = pd.Categorical(self.df["country"], categories=["United Kingdom", "United States", "Germany"])

        self.df["country_code"] = pd.Categorical(self.df["country_code"], categories=["US", "GB", "GE"])

        # Clean phone numbers using regex
        self.df["phone_number"] = self.df["phone_number"].replace({r"\(": "", r"\)": "", r"-": "", r" ": "", r"\.": ""}, regex=True)

        # Validate UUID length
        self.df["user_uuid"] = self.df["user_uuid"].astype(str)
        self.df.loc[self.df["user_uuid"].str.len() != 36, "user_uuid"] = np.nan

        # Drop rows with missing values and reset index
        self.df = self.df.dropna(subset=["user_uuid","country_code","country","date_of_birth","join_date"])

        return self.df

    def clean_card_data(self):
        """Clean card data in the DataFrame."""

        self.df = pd.read_csv("raw_cards_data.csv")
        # Clean 'card_provider' column
        remove_strings = [' 16 digit', ' 15 digit', ' 13 digit', ' 19 digit']
        self.df["card_provider"] = self.df["card_provider"].str.replace("|".join(remove_strings), "", regex=True)
        self.df["card_provider"] = self.df["card_provider"].str.upper()

        # Iterate through rows
        for index, row in self.df.iterrows():
            # Extract the "card_number expiry_date" value as a string and remove leading/trailing spaces
            card_expiry = str(row['card_number expiry_date']).strip()
            
            # Check if the value is not empty and is a string
            if card_expiry and isinstance(row['card_number expiry_date'], str):
                # Split the value into "card_number" and "expiry_date" using the first space as the partition
                split_values = card_expiry.split(' ', 1)
                
                # Update the DataFrame with the split values
                self.df.at[index, 'card_number'] = split_values[0]
                self.df.at[index, 'expiry_date'] = split_values[1]
        #remove non numeric charcters from card number       
        self.df["card_number"] = self.df["card_number"].str.replace(r'[^0-9]', '', regex=True)
        # Drop duplicate card_number values
        self.df = self.df.drop_duplicates(subset=['card_number'])
        # Drop null card_number values
        self.df = self.df.dropna(subset=["card_number"])
        # Convert 'expiry_date' to str
        self.df["expiry_date"] = self.df["expiry_date"].astype(str)
        self.df["date_payment_confirmed"] = self.df["date_payment_confirmed"].apply(self.date_parsing)
        self.df = self.df.dropna(subset=["date_payment_confirmed"])
        # Drop irrelevant columns
        #self.df = self.df.drop(self.df.columns[-2:], axis=1)

        return self.df

    def clean_store_data(self):
        """Clean store data in the DataFrame."""
        # Replace placeholders with NaN
        self.df.replace(["N/A", "NULL", "None", ""], pd.NA, inplace=True)

        # Clean address column
        self.df["address"] = self.df["address"].str.lower().str.strip()

        # Swap 'lat' and 'latitude' columns and rename
        self.df["lat"], self.df["latitude"] = self.df["latitude"], self.df["lat"]
        self.df.drop("latitude", axis=1, inplace=True)
        self.df.rename(columns={"lat": "latitude"}, inplace=True)

        # Convert 'staff_numbers' to numeric
        self.df['staff_numbers'] = self.df['staff_numbers'].str.replace('[^0-9]', '', regex=True)
        self.df["staff_numbers"] = pd.to_numeric(self.df["staff_numbers"]).astype("Int32")

        # Set 'store_type' and 'country_code' as categorical types
        self.df["store_type"] = pd.Categorical(self.df["store_type"], categories=["Web Portal", "Local", "Super Store", "Mall Kiosk", "Outlet"])
        self.df["country_code"] = pd.Categorical(self.df["country_code"], categories=["GB", "DE", "US"])

        # Replace and categorize 'continent'
        self.df["continent"] = self.df["continent"].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        self.df["continent"] = pd.Categorical(self.df["continent"], categories=["Europe", "America"])

        # Convert specified columns to numeric
        self.df[["latitude", "longitude"]] = self.df[["latitude", "longitude"]].apply(pd.to_numeric, errors="coerce")

        # Apply custom date parsing
        self.df["opening_date"] = self.df["opening_date"].apply(self.date_parsing)

        # Separate and clean 'Web Portal' rows
        web_portal_df = self.df[self.df["store_type"] == "Web Portal"]
        web_portal_cleaned = web_portal_df.dropna(subset=[col for col in web_portal_df.columns if col not in ["longitude", "latitude", "locality", "address"]])

        # Clean the rest of the DataFrame
        other_df = self.df[self.df["store_type"] != "Web Portal"]
        other_df_cleaned = other_df.dropna()

        # Concatenate and reset index
        self.df = pd.concat([web_portal_cleaned, other_df_cleaned]).reset_index(drop=True)

        # Renumber the 'index' column
        self.df["index"] = range(len(self.df))

        return self.df

    def convert_product_weights(self):
        conversions = {"kg": 1, "g": 1000, "ml": 1000, "oz": 0.0283495, "lb": 0.453592}

        def convert(weight):
            if isinstance(weight, str) and weight.strip():  # Check if the string is not empty or consists only of whitespace
                match = re.search(r"[a-zA-Z]+", weight)

                if match:
                    unit = weight[match.start():]
                    value_part = weight[:match.start()].strip()  # Extract the numeric part and strip whitespace

                    if value_part:
                        value = float(value_part)

                        if unit in conversions:
                            converted_value = value / conversions[unit]
                            return str(converted_value) + "kg"

            return None

        self.df["weight"] = self.df["weight"].apply(convert)
        return self.df

    def clean_products_data(self):

        self.df["product_name"] = self.df["product_name"].astype(str)

        self.df["product_price"] = self.df["product_price"].str.replace("£", "")
        self.df["product_price"] = pd.to_numeric(self.df["product_price"], errors="coerce")

        categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
        self.df["category"] = pd.Categorical(self.df["category"], categories=categories)

        # date
        self.df["date_added"] = self.df["date_added"].apply(self.date_parsing)

        self.df["removed"] = pd.Categorical(self.df["removed"], categories=["Still_avaliable", "Removed"])

        self.df = self.df.dropna(subset=["category","removed","product_price"])

        return self.df

    def clean_orders_data(self):

        self.df = self.df.drop(columns=["first_name", "last_name", "1", "level_0"])
        
        self.df["card_number"] = self.df["card_number"].astype(str)
        self.df["user_uuid"] = self.df["user_uuid"].astype(str)
        self.df.loc[self.df["user_uuid"].str.len() != 36, "user_uuid"] = np.nan

        self.df["product_quantity"] = pd.to_numeric(self.df["product_quantity"], errors="coerce")

        self.df = self.df.dropna()
        # Renumber the 'index' column
        self.df["index"] = range(len(self.df))

        return self.df

    def clean_date_events_data(self):

        self.df.rename(
            columns={
                "timestamp": "time",
                "month": "month",
                "year": "year",
                "day": "day",
                "time_period": "period",
            },
            inplace=True,
        )

        self.df["date"] = (self.df["year"] + "-" + self.df["month"] + "-" + self.df["day"]).apply(self.date_parsing)
        self.df = self.df.dropna()

        # Extract the time part from the datetime
        self.df.loc[:,"time"] = pd.to_datetime(self.df["time"], format="%H:%M:%S").dt.time

        self.df.loc[:,"period"] = self.df["period"].str.capitalize()
        periods = ["Evening", "Morning", "Midday", "Late_hours"]
        self.df.loc[:,"period"] = pd.Categorical(self.df["period"], categories=periods)

        self.df = self.df.dropna()
        return self.df
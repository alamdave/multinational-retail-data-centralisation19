# Data Extraction and Cleaning Toolkit

## Overview
This toolkit provides a comprehensive suite of tools for extracting, cleaning, and processing data from various sources including databases, PDFs, APIs, and S3 buckets. It is designed to handle a wide range of data formats and structures, making it an ideal choice for data analysts and engineers working with diverse data sets.

## Features
- **Database Operations**: Connect to SQL databases, list tables, and read data.
- **PDF Data Extraction**: Extract data from PDF files.
- **API Data Retrieval**: Fetch data from RESTful APIs.
- **AWS S3 Integration**: Download and process data from S3 buckets.
- **Data Cleaning**: Comprehensive data cleaning capabilities for various data types including user, card, store, product, and event data.

## Modules
1. **DataExtractor**: Handles data extraction from databases, PDFs, and APIs.
2. **DatabaseConnector**: Manages database connections and operations.
3. **DataCleaning**: Provides functions for cleaning and preprocessing data.

## Installation
To use this toolkit, you need to install the following Python packages:

pip install sqlalchemy pandas tabula-py requests boto3 pyyaml


## Usage

### Data Extraction
- **Database Operations**: Use `DataExtractor` to connect to a database and extract table data.
- **PDF Extraction**: Use `DataExtractor.retrieve_pdf_data(link)` to extract data from PDF files.
- **API Data Retrieval**: Use `DataExtractor.list_number_of_stores()` and `DataExtractor.retrieve_stores_data()` for API interactions.

### Database Connection
- **Setup**: Use `DatabaseConnector` with a YAML file containing database credentials.
- **Upload Data**: Use `DatabaseConnector.upload_to_db(table, table_name)` to upload data to a database.

### Data Cleaning
- **Initialization**: Instantiate `DataCleaning` with a pandas DataFrame.
- **Cleaning Operations**: Use methods like `clean_user_data()`, `clean_card_data()`, etc., to clean specific data types.

## Examples
```python
# Database Operations
extractor = DataExtractor(engine=my_engine)
table_data = extractor.read_rds_table("my_table")

# PDF Data Extraction
pdf_data = extractor.retrieve_pdf_data("http://example.com/myfile.pdf")

# API Data Retrieval
store_count = extractor.list_number_of_stores(api_endpoint, headers)
store_data = extractor.retrieve_stores_data(details_endpoint, store_count, headers)

# Data Cleaning
cleaner = DataCleaning(dataframe=my_dataframe)
cleaned_data = cleaner.clean_user_data()
```

## Dependencies
- Python 3.x
- SQLAlchemy
- Pandas
- Tabula-py
- Requests
- Boto3
- PyYAML

## Contributing
Contributions to enhance the toolkit are welcome. Please ensure that your code adheres to the project's coding standards and includes appropriate tests.

## License
This toolkit is released under the [MIT License](https://opensource.org/licenses/MIT).
```

You can copy and paste this content into a Markdown file (such as `README.md`) for your project.

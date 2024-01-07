# Project Title: Data Extraction and Cleaning Toolkit

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Modules](#modules)
- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [Contributing](#contributing)
- [License](#license)

## Overview
This toolkit provides a comprehensive suite of tools for extracting, cleaning, and processing data from various sources including databases, PDFs, APIs, and S3 buckets. Designed for data analysts and engineers, it handles a wide range of data formats and structures, streamlining the data preparation process.

### Project Aim
The aim of this project is to simplify the process of data extraction and cleaning, making it more efficient and less time-consuming. This toolkit is a result of extensive research and learning in data handling, showcasing the application of various Python libraries in real-world scenarios.

## Features
- **Database Operations**: Connect to SQL databases, list tables, and read data.
- **PDF Data Extraction**: Extract data from PDF files.
- **API Data Retrieval**: Fetch data from RESTful APIs.
- **AWS S3 Integration**: Download and process data from S3 buckets.
- **Data Cleaning**: Comprehensive data cleaning capabilities for various data types.

## Modules
1. **DataExtractor**: Handles data extraction from databases, PDFs, and APIs.
2. **DatabaseConnector**: Manages database connections and operations.
3. **DataCleaning**: Provides functions for cleaning and preprocessing data.

## Installation
Install the required Python packages:
bash
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

## File Structure
```
data-extraction-toolkit/
│
├── DataExtractor.py       # Module for data extraction
├── DatabaseConnector.py   # Module for database connection
├── DataCleaning.py        # Module for data cleaning
├── main.py                # Module to Execute functions
├── requirements.txt       # Required Python packages
└── README.md              # Documentation of the toolkit
```

## Contributing
Contributions to enhance the toolkit are welcome. Please ensure that your code adheres to the project's coding standards and includes appropriate tests.

## License
This toolkit is released under the [MIT License](https://opensource.org/licenses/MIT).

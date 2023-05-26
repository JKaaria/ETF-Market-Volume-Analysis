# Big Data Cloud Project

This project demonstrates a data pipeline for extracting and processing stock data using various AWS services.

## Project Overview

The project involves the following steps:

1. Data Extraction:
   - Use the `yfinance` library to download stock data for a list of ETF tickers.
   - Save each stock's data to a CSV file.
   - Upload the CSV file to Amazon S3 for storage.

2. Data Processing:
   - Download the CSV files for each stock from Amazon S3.
   - Perform data preprocessing and normalization on the downloaded data.
   - Save the processed data to new CSV files.
   - Upload the processed CSV files back to Amazon S3.

3. Data Analysis:
   - Download the processed CSV files from Amazon S3.
   - Merge the data into a single DataFrame.
   - Perform operations on the merged data to gain insights.

## Prerequisites

- Python 3.x
- AWS account with appropriate permissions
- AWS CLI (for running AWS CLI commands)

## Setup

1. Install the required Python packages:

2. Configure AWS CLI with your AWS access key and secret access key:

3. Create an S3 bucket for storing the data. Replace `<BUCKET_NAME>` with your desired bucket name:

4. Update the code files:
- Replace the placeholders in the code files with your own values, such as S3 bucket name, AWS access key, and secret access key.

## Usage

1. Data Extraction:
- Run the script `data_extraction.py` to extract stock data and store it in Amazon S3:
  ```
  python data_extraction.py
  ```

2. Data Processing:
- Run the script `data_processing.py` to download, process, and upload the stock data files:
  ```
  python data_processing.py
  ```

3. Data Analysis:
- Run the script `data_analysis.py` to download and analyze the processed data:
  ```
  python data_analysis.py
  ```

## License

This project is licensed under the [MIT License](LICENSE).
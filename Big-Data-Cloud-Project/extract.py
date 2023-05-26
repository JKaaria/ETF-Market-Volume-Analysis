import yfinance as yf
import boto3
import pandas as pd

# Step 1: Read the CSV file and extract tickers from column A
df = pd.read_csv('List of Top 100 ETFs.csv')
tickers = df['Symbol'].tolist()

# Step 2: Loop through the tickers and extract data using yfinance
for stock_ticker in tickers:
    # Extract data using yfinance
    data_yfinance = yf.download(stock_ticker)

    # Save data to CSV file with ticker name
    filename = f'{stock_ticker}.csv'
    data_yfinance.to_csv(filename)

    # Store the extracted data in Amazon S3
    s3 = boto3.client('s3')
    bucket_name = 'big-data-cloud-project-2023'
    s3.upload_file(filename, bucket_name, f'{stock_ticker}/{filename}')

print("Data extraction and storage completed successfully!")

import boto3
import pandas as pd
import os

# Set up the S3 client
s3 = boto3.client('s3')

# Read the list of tickers from a CSV file
tickers_df = pd.read_csv('List of Top 100 ETFs.csv')
tickers = tickers_df['Symbol'].tolist()

# Loop through each ticker and process the data
for ticker in tickers:
    # Download the CSV file for the current ticker
    csv_path = f'{ticker}/{ticker}.csv'
    csv_file = f'{ticker}.csv'
    s3.download_file('big-data-cloud-project-2023', csv_path, csv_file)

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file)

    # Handle missing values
    df.fillna(0, inplace=True)

    # Perform data normalization
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    df[numeric_columns] = (df[numeric_columns] - df[numeric_columns].min()) / (df[numeric_columns].max() - df[numeric_columns].min())

    # Adjust for data consistency
    df['Symbol'] = ticker.upper()

    # Create the directory if it doesn't exist
    os.makedirs(ticker, exist_ok=True)

    # Save the processed data to a new CSV file
    processed_csv_path = f'{ticker}/processed_{ticker}.csv'
    df.to_csv(processed_csv_path, index=False)

    # Upload the processed CSV file back to S3
    s3.upload_file(processed_csv_path, 'big-data-cloud-project-2023', processed_csv_path)

    print(f"Data processing completed for {ticker}")

    # Delete the downloaded CSV file
    os.remove(csv_file)

print("All data processing completed successfully!")

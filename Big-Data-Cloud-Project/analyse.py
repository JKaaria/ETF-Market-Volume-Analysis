import csv
import boto3
import pandas as pd

# Define the S3 bucket name
S3_BUCKET = 'big-data-cloud-project-2023'

# Path to the CSV file containing the symbols
symbols_csv_file = 'List of Top 100 ETFs.csv'

# Read the symbols from the CSV file
symbols_df = pd.read_csv(symbols_csv_file)
symbols = symbols_df['Symbol'].tolist()

# Create a session using your AWS credentials
session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
)

# Create an S3 client using the session
s3 = session.client('s3')

# List to store price history for all ETF tickers
price_history = []

# Loop through the symbols and process the corresponding CSV files
for symbol in symbols:
    # Define the path to the processed CSV file
    csv_file_path = f's3://{S3_BUCKET}/{symbol}/processed_{symbol}.csv'
    
    try:
        # Download the CSV file from S3
        local_csv_file = f'/Users/jeremykaaria/Documents/Work-Projects/Big-Data-Cloud-Project/{symbol}/{symbol}.csv'
        s3.download_file(S3_BUCKET, f'{symbol}/processed_{symbol}.csv', local_csv_file)
        
        # Process the CSV file
        with open(local_csv_file, 'r') as file:
            reader = csv.reader(file)
            
            # Skip the header row
            header = next(reader)
            
            # Get the number of columns
            num_columns = len(header)
            
            # Process each row in the CSV file
            for row in reader:
                # Adjust the number of columns if necessary
                if len(row) != num_columns:
                    row = row[:num_columns]
                price_history.append([symbol] + row[1:])  # Append ticker symbol to each row of price history
    
    except Exception as e:
        print(f'Error processing symbol {symbol}: {str(e)}')

# Create a DataFrame with the merged price history
column_names = ['Symbol'] + header[1:]
price_history_df = pd.DataFrame(price_history, columns=column_names)

# Define the output CSV file path
output_csv_file = '/Users/jeremykaaria/Documents/Work-Projects/Big-Data-Cloud-Project/price_history.csv'

# Save merged price history to CSV
price_history_df.to_csv(output_csv_file, index=False)

# Upload the CSV file to S3
s3.upload_file(output_csv_file, S3_BUCKET, 'price_history.csv')
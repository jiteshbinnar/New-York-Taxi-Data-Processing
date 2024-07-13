import requests
import os
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

# Function to download and save Parquet file as CSV
def download_parquet_as_csv(url, save_path):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Read the content of the response
            parquet_file = BytesIO(response.content)
            
            # Read Parquet file using pyarrow
            table = pq.read_table(parquet_file)
            
            # Save the Parquet table to a local file
            pq.write_table(table, save_path)
            
            # Read Parquet file using Pandas
            df = pd.read_parquet(save_path)
            
            # Define CSV path for saving
            csv_path = save_path.replace('.parquet', '.csv')
            
            # Write DataFrame to CSV
            df.to_csv(csv_path, index=False)  # Index=False to exclude DataFrame index
            
            print(f"Parquet file downloaded and saved as CSV: {csv_path}")
        else:
            print(f"Failed to download Parquet file. Status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def main():
    # Ensure the directory to save the Parquet file exists
    save_dir = 'files'
    os.makedirs(save_dir, exist_ok=True)
    
    # Specify where to save the downloaded Parquet file locally
    save_path = os.path.join(save_dir, 'yellow_tripdata_2019.parquet')
    
    # URL of the Parquet file
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet'
    
    # Call the function to download and save the Parquet file
    download_parquet_as_csv(url, save_path)

if __name__ == "__main__":
    main()

from sqlalchemy import create_engine
import urllib.parse
from project_scripts.processing import data_processing
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Accessing database credentials
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Use these credentials to connect to your database


# Use these credentials to connect to your database


# URL-encode the password
password = urllib.parse.quote_plus(db_password)

# Connection string
connection_string = f'mysql+mysqlconnector://{db_user}:{password}@{db_host}/{db_name}'

# Create the engine with pool size configuration
sql_engine = create_engine(connection_string, pool_size=5, max_overflow=10)

def load_data_to_database():
    try:
        # Use a context manager for the connection
        with sql_engine.connect() as connection:
            # Start a transaction
            with connection.begin():
                print("Connection successful!")
                
                load_df = data_processing()  # Assuming this returns a DataFrame

                # Load data into SQL using the connection in chunks
                load_df.to_sql('taxi_trip_data', con=connection, if_exists='append', index=False, chunksize=1000)
                print("Data loaded successfully!")

    except Exception as e:
        print(f"Error: {e}")

def main():
    load_data_to_database()

if __name__ == "__main__":
    main()

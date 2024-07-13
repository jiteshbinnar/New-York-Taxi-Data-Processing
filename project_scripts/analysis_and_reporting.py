import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector  # Assuming MySQL database
from dotenv import load_dotenv
import os

load_dotenv()

# Load environment variables from .env file
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Connect to MySQL database
conn = mysql.connector.connect(
host = db_host,
username=db_user,
password=db_password,
database=db_name
)

# Execute SQL queries
queries = {
    "peak_hours": """
        SELECT
            HOUR(tpep_pickup_datetime) AS pickup_hour,
            COUNT(*) AS num_trips
        FROM
            taxi_trip_data
        GROUP BY
            pickup_hour
        ORDER BY
            num_trips DESC
        LIMIT 5;
    """,
    "passenger_vs_fare": """
        SELECT
            passenger_count,
            AVG(fare_amount) AS avg_fare
        FROM
            taxi_trip_data
        GROUP BY
            passenger_count
        ORDER BY
            passenger_count;
    """,
    "usage_trends": """
        SELECT
            DATE_FORMAT(tpep_pickup_datetime, '%Y-%m') AS pickup_month,
            COUNT(*) AS num_trips
        FROM
            taxi_trip_data
        GROUP BY
            pickup_month
        ORDER BY
            pickup_month;
    """
}

dfs = {}  # Dictionary to hold result DataFrames

# Execute queries and store results in DataFrames
for key, query in queries.items():
    df = pd.read_sql_query(query, conn)
    dfs[key] = df

# Close database connection
conn.close()

# Plotting peak hours
plt.figure(figsize=(10, 6))
plt.bar(dfs['peak_hours']['pickup_hour'], dfs['peak_hours']['num_trips'], color='skyblue')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Trips')
plt.title('Peak Hours for Taxi Usage')
plt.xticks(range(24))
plt.grid(True)
plt.show()

# Plotting passenger count vs. fare
plt.figure(figsize=(8, 6))
plt.bar(dfs['passenger_vs_fare']['passenger_count'], dfs['passenger_vs_fare']['avg_fare'], color='lightgreen')
plt.xlabel('Passenger Count')
plt.ylabel('Average Fare ($)')
plt.title('Effect of Passenger Count on Trip Fare')
plt.grid(True)
plt.show()

# Plotting usage trends over the year
plt.figure(figsize=(12, 6))
plt.plot(dfs['usage_trends']['pickup_month'], dfs['usage_trends']['num_trips'], marker='o', color='orange')
plt.xlabel('Month')
plt.ylabel('Number of Trips')
plt.title('Usage Trends over the Year')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

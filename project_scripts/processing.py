import pandas as pd





def data_processing():
    df = pd.read_csv("files/yellow_tripdata_2019.csv")
    # Derive new columns such as trip duration
    df['tpep_pickup_datetime']=pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']=pd.to_datetime(df['tpep_dropoff_datetime'])
    df['trip_duration']=df['tpep_dropoff_datetime']-df['tpep_pickup_datetime']
    df['trip_duration']=df['trip_duration'].astype(str).str.split(" ").str[-1]

    # Derive new columns such as avg speed
    df['avg_speed']= df.apply(lambda a:a['trip_distance']/(pd.to_timedelta(a['trip_duration']).total_seconds()/3600) if pd.to_timedelta(a['trip_duration']).total_seconds() !=0 else float('NaN') ,axis=1)
    
    # Removed trips that have missing or corrupt data.
    del df['airport_fee']
    df_clean=df.dropna().drop_duplicates()

    return df_clean


def aggregate_data():
    agg_df=data_processing()
    # Aggregate data to calculate total trips and average fare per day.
    
    aggregated = agg_df.groupby(agg_df['tpep_pickup_datetime'].astype(str).str.split(" ").str[0]).agg(
    total_trips=('VendorID', 'count'),
    average_fare=('fare_amount', 'mean')
    ).reset_index()
    # Display the result
    print(aggregated)

    
 


def main():
    data_processing()
    aggregate_data()


if __name__=="__main__":

    main()    



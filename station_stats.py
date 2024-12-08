from utils_local import *
import os
import io
import boto3
import logging
import pandas as pd

# Initialize S3 client
s3_client = boto3.client('s3')

def station_stats(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str,
        file_format: str = 'json'
):
    """
    This function returns station statistics for a given time period and model.
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    :param file_format: str: 'json' or 'parquet' (default: 'json')
    """
    
    # model types: station_level, postcode_level, suburb_level, district_level, city_level
    # model codes: station_id, postcode, suburb, district, city

    # Load data
    main_folder = 'analytics/snapshots'
    
    # Handle Parquet file reading
    if file_format == 'parquet':
        try:
            # Read parquet file from S3
            response = s3_client.get_object(
                Bucket='bicingdata',
                Key='2023/data.parquet'
            )
            body_data = response['Body'].read()
            parquet_file = io.BytesIO(body_data)
            raw_data = pd.read_parquet(parquet_file)
            
            # Convert timestamp and filter by date range
            raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])
            date_mask = (raw_data['timestamp'] >= pd.to_datetime(from_date)) & \
                       (raw_data['timestamp'] <= pd.to_datetime(to_date))
            raw_data = raw_data[date_mask].copy()
            
            if len(raw_data) == 0:
                raise ValueError(f"No data found in Parquet file for date range {from_date} to {to_date}")

            # Process each row and extract station data
            all_stations = []
            for _, row in raw_data.iterrows():
                try:
                    stations = row['data']['stations']
                    for station in stations:
                        station_record = {
                            'station_id': str(station.get('station_id', '')),
                            'num_bikes_available': station.get('num_bikes_available', 0),
                            'num_docks_available': station.get('num_docks_available', 0),
                            'timestamp_file': row['timestamp']
                        }
                        all_stations.append(station_record)
                except Exception as e:
                    continue
            
            if not all_stations:
                raise ValueError("No valid station data found after processing")
            
            # Create DataFrame with only the needed columns
            stations_data = pd.DataFrame(all_stations)
            
        except Exception as e:
            raise ValueError(f"Failed to retrieve data from S3: {str(e)}")
    else:
        # Original JSON processing
        dates = list_folders(main_folder)
        files = list_all_files(main_folder, dates)
        files = [f for f in files if f.endswith('.json')]
        files = filter_input_by_timeframe(files, from_date, to_date)
        stations_data = json_to_dataframe(files)

    duration_segs = (pd.to_datetime(to_date) - pd.to_datetime(from_date)).total_seconds()
    
    stations_master = get_station_information()

    stations = get_stations(model, model_code)
    stations_data = stations_data[stations_data['station_id'].isin(stations)]
    stations_master['station_id'] = stations_master['station_id'].astype(int).astype(str)
    stations_master = stations_master[stations_master['station_id'].isin(stations)]

    stations_master = stations_master[['station_id', 'capacity']]
    stations_data = stations_data[['station_id', 'num_bikes_available', 'num_docks_available', 'timestamp_file']]

    stations_data = pd.merge(stations_data, stations_master, on='station_id', how='inner')

    #########################################################
    ### METRICS
    #########################################################

    stations_data_agg = stations_data.groupby('station_id').agg({
        'num_bikes_available': 'mean',
        'num_docks_available': 'mean'
    }).reset_index()
    stations_data_agg = stations_data_agg.rename(columns={
        'num_bikes_available': 'average_bikes_available',
        'num_docks_available': 'average_docks_available'
    })
    
    #########################################################
    ### AVAILABILITY METRICS
    #########################################################
    # Calculate percentage and percentile of time with 0 bikes available
    zero_bikes_pct = stations_data.groupby('station_id').agg({
        'num_bikes_available': lambda x: (x == 0).mean() 
    }).reset_index()
    zero_bikes_pct = zero_bikes_pct.rename(columns={
        'num_bikes_available': 'pct_time_zero_bikes'
    })
    zero_bikes_pct['time_zero_bikes_percentile'] = zero_bikes_pct['pct_time_zero_bikes'].rank(pct=True, method='dense')
    
    # Calculate percentage and percentile of time with 0 docks available 
    zero_docks_pct = stations_data.groupby('station_id').agg({
        'num_docks_available': lambda x: (x == 0).mean() 
    }).reset_index()
    zero_docks_pct = zero_docks_pct.rename(columns={
        'num_docks_available': 'pct_time_zero_docks'
    })
    zero_docks_pct['time_zero_docks_percentile'] = zero_docks_pct['pct_time_zero_docks'].rank(pct=True, method='dense')
    
    # Merge with main dataframe
    stations_data_agg = pd.merge(stations_data_agg, zero_bikes_pct, on='station_id', how='inner')
    stations_data_agg = pd.merge(stations_data_agg, zero_docks_pct, on='station_id', how='inner')

    events = calculate_use_events(stations_data, duration_segs)
    stations_data_agg = pd.merge(stations_data_agg, events, on='station_id', how='inner')
    
    # Convert DataFrame to dictionary with native Python types
    return stations_data_agg.to_dict(orient='records')

def station_stats_parquet(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str
):
    """
    Convenience function to get station statistics from Parquet files.
    This is equivalent to calling station_stats with file_format='parquet'.
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    """
    return station_stats(from_date, to_date, model, model_code, file_format='parquet')

def get_snapshot_stats(
        timestamp: str,
        model: str,
        model_code: str,
        file_format: str = 'json'
):
    """
    Get station statistics for a specific timestamp.
    :param timestamp: str: target timestamp
    :param model: str: model type
    :param model_code: str: model code
    :param file_format: str: 'json' or 'parquet' (default: 'json')
    """
    main_folder = 'analytics/snapshots'
    single_parquet = 'data/2023/data.parquet'
    
    # Handle Parquet file reading
    if file_format == 'parquet':
        if os.path.exists(single_parquet):
            try:
                raw_data = pd.read_parquet(single_parquet)
                
                # Convert timestamp to datetime for comparison
                raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])
                target_time = pd.to_datetime(timestamp)
                
                # Find closest timestamp
                raw_data['time_diff'] = abs(raw_data['timestamp'] - target_time)
                closest_row = raw_data.loc[raw_data['time_diff'].idxmin()]
                
                # Process station data from closest timestamp
                all_stations = []
                try:
                    stations = closest_row['data']['stations']
                    for station in stations:
                        station_record = {
                            'station_id': str(station.get('station_id', '')),
                            'num_bikes_available': station.get('num_bikes_available', 0),
                            'num_docks_available': station.get('num_docks_available', 0),
                            'timestamp_file': closest_row['timestamp']
                        }
                        all_stations.append(station_record)
                except Exception as e:
                    raise ValueError(f"Error processing station data: {str(e)}")
                
                if not all_stations:
                    raise ValueError("No valid station data found")
                
                stations_data = pd.DataFrame(all_stations)
                
            except Exception as e:
                raise ValueError(f"Error reading Parquet file: {str(e)}")
        else:
            raise ValueError(f"Parquet file not found at {single_parquet}")
    else:
        # Original JSON processing
        dates = list_folders(main_folder)
        files = list_all_files(main_folder, dates)
        files = [f for f in files if f.endswith('.json')]
        closest_file = filter_input_by_timestamp(files, timestamp)[0]
        stations_data = json_to_dataframe([closest_file])
    
    stations = get_stations(model, model_code)
    stations_data = stations_data[stations_data['station_id'].isin(stations)]
    
    return stations_data.to_dict(orient='records')

def get_snapshot_stats_parquet(
        timestamp: str,
        model: str,
        model_code: str
):
    """
    Convenience function to get snapshot statistics from Parquet files.
    This is equivalent to calling get_snapshot_stats with file_format='parquet'.
    :param timestamp: str: target timestamp
    :param model: str: model type
    :param model_code: str: model code
    """
    return get_snapshot_stats(timestamp, model, model_code, file_format='parquet')

def calculate_use_events(df, duration_segs):
    """
    This function returns the number of use in and use out events by station.
    :param df: dataframe: dataframe with the station data
    :param duration_segs: int: duration in seconds
    :return: dataframe: dataframe with the number of use in and use out events by station
    """

    df['use_in'] = df.groupby('station_id')['num_bikes_available'].transform(lambda x: abs(x.diff().shift(-1)) * (x.diff().shift(-1) < 0))
    df['use_out'] = df.groupby('station_id')['num_bikes_available'].transform(lambda x: abs(x.diff().shift(-1)) * (x.diff().shift(-1) > 0))
    df['events'] = df['use_in'] + df['use_out']
    df = df[['station_id', 'events', 'use_in', 'use_out']]
    df = df.groupby('station_id').sum().reset_index()
    day = 24 * 60 * 60
    df['use_in_per_day'] = (df['use_in'] / duration_segs) * day
    df['use_out_per_day'] = (df['use_out'] / duration_segs) * day
    df['events_per_day'] = (df['events'] / duration_segs) * day

    stations_master = get_station_information()[['station_id', 'capacity']]
    stations_master['station_id'] = stations_master['station_id'].astype(int).astype(str)
    df = pd.merge(df, stations_master, on='station_id', how='inner')

    df['use_in_per_day_capacity'] = df['use_in_per_day'] / df['capacity']
    df['use_out_per_day_capacity'] = df['use_out_per_day'] / df['capacity']
    df['events_per_day_capacity'] = df['events_per_day'] / df['capacity']

    # Calculate the percentile of use 

    df['events_percentile'] = df['events'].rank(pct=True, method='dense')
    df['use_in_percentile'] = df['use_in'].rank(pct=True, method='dense')
    df['use_out_percentile'] = df['use_out'].rank(pct=True, method='dense')

    df['events_per_day_capacity_percentile'] = df['events_per_day_capacity'].rank(pct=True, method='dense')
    df['use_in_per_day_capacity_percentile'] = df['use_in_per_day_capacity'].rank(pct=True, method='dense')
    df['use_out_per_day_capacity_percentile'] = df['use_out_per_day_capacity'].rank(pct=True, method='dense')


    return df
    





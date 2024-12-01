from utils_local import *

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

    # Load data: To change in cloud environment
    main_folder = 'analytics/snapshots'
    
    dates = list_folders(main_folder)
    files = list_all_files(main_folder, dates)
    
    # Filter files by format
    if file_format == 'parquet':
        files = [f for f in files if f.endswith('.parquet')]
    else:
        files = [f for f in files if f.endswith('.json')]
    
    files = filter_input_by_timeframe(files, from_date, to_date)

    duration_segs = (pd.to_datetime(to_date) - pd.to_datetime(from_date)).total_seconds()
    
    # Read data based on file format
    if file_format == 'parquet':
        stations_data = read_parquet_files(files)
    else:
        stations_data = json_to_dataframe(files)
    
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
    dates = list_folders(main_folder)
    files = list_all_files(main_folder, dates)
    
    # Filter files by format
    if file_format == 'parquet':
        files = [f for f in files if f.endswith('.parquet')]
    else:
        files = [f for f in files if f.endswith('.json')]
    
    closest_file = filter_input_by_timestamp(files, timestamp)[0]
    
    # Read data based on file format
    if file_format == 'parquet':
        stations_data = read_parquet_files([closest_file])
    else:
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
    





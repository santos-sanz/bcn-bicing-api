from utils_local import *

def station_stats(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str
):
    """
    This function returns the number of stations, the sum of bikes available and the sum of docks available for a given timestamp, model and model code.
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    """
    
    # model types: station_level, postcode_level, suburb_level, district_level, city_level
    # model codes: station_id, postcode, suburb, district, city

    # Load data: To change in cloud environment
    main_folder = 'analytics/snapshots'
    
    dates = list_folders(main_folder)
    files = list_all_files(main_folder, dates)
    files = filter_input_by_timeframe(files, from_date, to_date)

    duration_segs = (pd.to_datetime(to_date) - pd.to_datetime(from_date)).total_seconds()
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

    events = calculate_use_events(stations_data, duration_segs)

    stations_data_agg = pd.merge(stations_data_agg, events, on='station_id', how='inner')
    


    return  stations_data_agg


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

    df['events_percentile'] = df['events'].rank(pct=True)
    df['use_in_percentile'] = df['use_in'].rank(pct=True)
    df['use_out_percentile'] = df['use_out'].rank(pct=True)

    df['events_per_day_capacity_percentile'] = df['events_per_day_capacity'].rank(pct=True)
    df['use_in_per_day_capacity_percentile'] = df['use_in_per_day_capacity'].rank(pct=True)
    df['use_out_per_day_capacity_percentile'] = df['use_out_per_day_capacity'].rank(pct=True)


    return df
    




